/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/experimental/cudf/expression/BloomFilterKernels.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_device_view.cuh>
#include <cudf/dictionary/dictionary_column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/strings/string_view.cuh>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>

#include <rmm/device_buffer.hpp>

#include <cub/device/device_for.cuh>
#include <thrust/iterator/counting_iterator.h>

#include <array>
#include <cstring>
#include <vector>

namespace facebook::velox::cudf_velox {
namespace {

constexpr uint8_t kBloomFilterV1 = 1;
constexpr int32_t kSerializedHeaderSize =
    sizeof(uint8_t) + sizeof(int32_t);

__device__ uint64_t twangMix64(uint64_t key) {
  key = (~key) + (key << 21);
  key ^= key >> 24;
  key = key + (key << 3) + (key << 8);
  key ^= key >> 14;
  key = key + (key << 2) + (key << 4);
  key ^= key >> 28;
  key += key << 31;
  return key;
}

struct BloomFilterMightContainFunctor {
  const uint64_t* input;
  const uint64_t* bloomBits;
  bool* output;
  uint32_t bloomSize;

  __device__ void operator()(int32_t row) const {
    const auto hash = twangMix64(input[row]);
    const auto mask = (uint64_t{1} << (hash & 63)) |
        (uint64_t{1} << ((hash >> 6) & 63)) |
        (uint64_t{1} << ((hash >> 12) & 63)) |
        (uint64_t{1} << ((hash >> 18) & 63));
    const auto index = static_cast<uint32_t>((hash >> 24) & (bloomSize - 1));
    output[row] = (bloomBits[index] & mask) == mask;
  }
};

__device__ uint32_t readUint32LittleEndian(const char* data) {
  uint32_t value = 0;
#pragma unroll
  for (int32_t byte = 0; byte < sizeof(value); ++byte) {
    value |= static_cast<uint32_t>(
                 static_cast<uint8_t>(data[byte]))
        << (byte * 8);
  }
  return value;
}

__device__ uint64_t readUint64LittleEndian(const char* data) {
  uint64_t value = 0;
#pragma unroll
  for (int32_t byte = 0; byte < sizeof(value); ++byte) {
    value |= static_cast<uint64_t>(
                 static_cast<uint8_t>(data[byte]))
        << (byte * 8);
  }
  return value;
}

struct DynamicBloomFilterMightContainFunctor {
  cudf::column_device_view serializedBloomFilters;
  const uint64_t* input;
  bool* output;
  bool dictionaryEncoded;

  __device__ void operator()(int32_t row) const {
    if (serializedBloomFilters.is_null(row)) {
      output[row] = false;
      return;
    }

    const auto serialized = dictionaryEncoded
        ? serializedBloomFilters.child(
              cudf::dictionary_column_view::keys_column_index)
              .element<cudf::string_view>(
                  serializedBloomFilters.element<cudf::dictionary32>(row)
                      .value())
        : serializedBloomFilters.element<cudf::string_view>(row);
    if (serialized.size_bytes() < kSerializedHeaderSize ||
        static_cast<uint8_t>(serialized.data()[0]) != kBloomFilterV1) {
      output[row] = false;
      return;
    }

    const auto bloomSize =
        readUint32LittleEndian(serialized.data() + sizeof(uint8_t));
    const auto serializedSize = static_cast<uint64_t>(kSerializedHeaderSize) +
        static_cast<uint64_t>(bloomSize) * sizeof(uint64_t);
    if (bloomSize == 0 || (bloomSize & (bloomSize - 1)) != 0 ||
        serializedSize > serialized.size_bytes()) {
      output[row] = false;
      return;
    }

    const auto hash = twangMix64(input[row]);
    const auto mask = (uint64_t{1} << (hash & 63)) |
        (uint64_t{1} << ((hash >> 6) & 63)) |
        (uint64_t{1} << ((hash >> 12) & 63)) |
        (uint64_t{1} << ((hash >> 18) & 63));
    const auto index = static_cast<uint32_t>((hash >> 24) & (bloomSize - 1));
    const auto* bloomBits =
        serialized.data() + kSerializedHeaderSize + index * sizeof(uint64_t);
    output[row] = (readUint64LittleEndian(bloomBits) & mask) == mask;
  }
};

struct BloomFilterInsertFunctor {
  const uint64_t* input;
  unsigned long long* bloomBits;
  uint32_t bloomSize;

  __device__ void operator()(int32_t row) const {
    const auto hash = twangMix64(input[row]);
    const auto mask = (uint64_t{1} << (hash & 63)) |
        (uint64_t{1} << ((hash >> 6) & 63)) |
        (uint64_t{1} << ((hash >> 12) & 63)) |
        (uint64_t{1} << ((hash >> 18) & 63));
    const auto index = static_cast<uint32_t>((hash >> 24) & (bloomSize - 1));
    atomicOr(bloomBits + index, static_cast<unsigned long long>(mask));
  }
};

struct BloomFilterMergeFunctor {
  const char* inputChars;
  const cudf::size_type* offsets;
  char* outputBits;
  cudf::size_type numFilters;
  cudf::size_type bloomBytes;

  __device__ void operator()(int32_t byte) const {
    uint8_t merged = 0;
    for (cudf::size_type row = 0; row < numFilters; ++row) {
      merged |= static_cast<uint8_t>(
          inputChars[offsets[row] + kSerializedHeaderSize + byte]);
    }
    outputBits[byte] = static_cast<char>(merged);
  }
};

std::unique_ptr<cudf::column> makeSerializedBloomFilterColumn(
    rmm::device_buffer bloomBits,
    int32_t bloomSize,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  const auto bloomBytes = bloomSize * sizeof(uint64_t);
  const auto serializedSize = kSerializedHeaderSize + bloomBytes;
  std::array<char, kSerializedHeaderSize> header{};
  header[0] = static_cast<char>(kBloomFilterV1);
  std::memcpy(header.data() + sizeof(uint8_t), &bloomSize, sizeof(bloomSize));

  rmm::device_buffer chars(serializedSize, stream, mr);
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      chars.data(),
      header.data(),
      header.size(),
      cudaMemcpyHostToDevice,
      stream.value()));
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      static_cast<char*>(chars.data()) + kSerializedHeaderSize,
      bloomBits.data(),
      bloomBytes,
      cudaMemcpyDeviceToDevice,
      stream.value()));

  std::array<cudf::size_type, 2> offsets{
      0, static_cast<cudf::size_type>(serializedSize)};
  rmm::device_buffer deviceOffsets(
      offsets.size() * sizeof(cudf::size_type), stream, mr);
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      deviceOffsets.data(),
      offsets.data(),
      deviceOffsets.size(),
      cudaMemcpyHostToDevice,
      stream.value()));
  stream.synchronize();

  auto offsetsColumn = std::make_unique<cudf::column>(
      cudf::data_type{cudf::type_id::INT32},
      offsets.size(),
      std::move(deviceOffsets),
      rmm::device_buffer{},
      0);
  return cudf::make_strings_column(
      1,
      std::move(offsetsColumn),
      std::move(chars),
      0,
      rmm::device_buffer{});
}

} // namespace

std::unique_ptr<cudf::column> bloomFilterMightContain(
    const cudf::column_view& input,
    const uint64_t* bloomBits,
    int32_t bloomSize,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  CUDF_EXPECTS(
      input.type().id() == cudf::type_id::INT64 ||
          input.type().id() == cudf::type_id::UINT64,
      "Bloom filter input must be a signed or unsigned 64-bit integer");
  CUDF_EXPECTS(bloomBits != nullptr, "Bloom filter bits must not be null");
  CUDF_EXPECTS(
      bloomSize > 0 && (bloomSize & (bloomSize - 1)) == 0,
      "Bloom filter size must be a positive power of two");

  auto nullMask = cudf::copy_bitmask(input, stream, mr);
  auto output = cudf::make_fixed_width_column(
      cudf::data_type{cudf::type_id::BOOL8},
      input.size(),
      std::move(nullMask),
      input.null_count(),
      stream,
      mr);
  if (input.is_empty()) {
    return output;
  }

  BloomFilterMightContainFunctor op{
      input.data<uint64_t>(),
      bloomBits,
      output->mutable_view().data<bool>(),
      static_cast<uint32_t>(bloomSize)};
  cub::DeviceFor::ForEachN(
      thrust::counting_iterator<int32_t>(0),
      input.size(),
      op,
      stream.value());
  return output;
}

std::unique_ptr<cudf::column> bloomFilterMightContain(
    const cudf::column_view& serializedBloomFilters,
    const cudf::column_view& input,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  CUDF_EXPECTS(
      serializedBloomFilters.type().id() == cudf::type_id::STRING ||
          serializedBloomFilters.type().id() ==
              cudf::type_id::DICTIONARY32,
      "Bloom filter input must be VARBINARY or dictionary-encoded VARBINARY");
  const bool dictionaryEncoded =
      serializedBloomFilters.type().id() == cudf::type_id::DICTIONARY32;
  if (dictionaryEncoded) {
    CUDF_EXPECTS(
        cudf::dictionary_column_view(serializedBloomFilters)
                .keys_type()
                .id() == cudf::type_id::STRING,
        "Bloom filter dictionary keys must be VARBINARY");
  }
  CUDF_EXPECTS(
      input.type().id() == cudf::type_id::INT64 ||
          input.type().id() == cudf::type_id::UINT64,
      "Bloom filter value must be a signed or unsigned 64-bit integer");
  CUDF_EXPECTS(
      serializedBloomFilters.size() == input.size(),
      "Bloom filter and value columns must have matching sizes");

  auto [nullMask, nullCount] = cudf::bitmask_and(
      cudf::table_view{{serializedBloomFilters, input}}, stream, mr);
  auto output = cudf::make_fixed_width_column(
      cudf::data_type{cudf::type_id::BOOL8},
      input.size(),
      std::move(nullMask),
      nullCount,
      stream,
      mr);
  if (input.is_empty()) {
    return output;
  }

  auto bloomFiltersDeviceView =
      cudf::column_device_view::create(serializedBloomFilters, stream);
  DynamicBloomFilterMightContainFunctor op{
      *bloomFiltersDeviceView,
      input.data<uint64_t>(),
      output->mutable_view().data<bool>(),
      dictionaryEncoded};
  cub::DeviceFor::ForEachN(
      thrust::counting_iterator<int32_t>(0),
      input.size(),
      op,
      stream.value());
  return output;
}

std::unique_ptr<cudf::column> buildSerializedBloomFilter(
    const cudf::column_view& input,
    int32_t bloomSize,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  CUDF_EXPECTS(
      input.type().id() == cudf::type_id::INT64 ||
          input.type().id() == cudf::type_id::UINT64,
      "Bloom filter input must be a signed or unsigned 64-bit integer");
  CUDF_EXPECTS(
      input.null_count() == 0, "Bloom filter input must not contain nulls");
  CUDF_EXPECTS(
      bloomSize > 0 && (bloomSize & (bloomSize - 1)) == 0,
      "Bloom filter size must be a positive power of two");

  rmm::device_buffer bloomBits(
      bloomSize * sizeof(uint64_t), stream, mr);
  CUDF_CUDA_TRY(cudaMemsetAsync(
      bloomBits.data(), 0, bloomBits.size(), stream.value()));
  if (!input.is_empty()) {
    BloomFilterInsertFunctor op{
        input.data<uint64_t>(),
        static_cast<unsigned long long*>(bloomBits.data()),
        static_cast<uint32_t>(bloomSize)};
    cub::DeviceFor::ForEachN(
        thrust::counting_iterator<int32_t>(0),
        input.size(),
        op,
        stream.value());
  }
  return makeSerializedBloomFilterColumn(
      std::move(bloomBits), bloomSize, stream, mr);
}

std::unique_ptr<cudf::column> mergeSerializedBloomFilters(
    const cudf::column_view& input,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  CUDF_EXPECTS(
      input.type().id() == cudf::type_id::STRING,
      "Bloom filter merge input must be VARBINARY");
  CUDF_EXPECTS(
      input.size() > 0, "Bloom filter merge requires at least one input");
  CUDF_EXPECTS(
      input.null_count() == 0,
      "Bloom filter merge does not support null inputs");

  const auto strings = cudf::strings_column_view(input);
  const auto offsets = strings.offsets();
  std::vector<cudf::size_type> hostOffsets(input.size() + 1);
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      hostOffsets.data(),
      offsets.data<cudf::size_type>(),
      hostOffsets.size() * sizeof(cudf::size_type),
      cudaMemcpyDeviceToHost,
      stream.value()));

  std::array<char, kSerializedHeaderSize> header{};
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      header.data(),
      strings.chars_begin(stream),
      header.size(),
      cudaMemcpyDeviceToHost,
      stream.value()));
  stream.synchronize();

  CUDF_EXPECTS(
      static_cast<uint8_t>(header[0]) == kBloomFilterV1,
      "Unsupported BloomFilter version");
  int32_t bloomSize = 0;
  std::memcpy(
      &bloomSize, header.data() + sizeof(uint8_t), sizeof(bloomSize));
  CUDF_EXPECTS(
      bloomSize > 0 && (bloomSize & (bloomSize - 1)) == 0,
      "Bloom filter size must be a positive power of two");

  const auto bloomBytes =
      static_cast<cudf::size_type>(bloomSize * sizeof(uint64_t));
  const auto serializedSize = kSerializedHeaderSize + bloomBytes;
  for (cudf::size_type row = 0; row < input.size(); ++row) {
    CUDF_EXPECTS(
        hostOffsets[row + 1] - hostOffsets[row] == serializedSize,
        "Bloom filter merge inputs must have matching sizes");
  }

  rmm::device_buffer bloomBits(bloomBytes, stream, mr);
  BloomFilterMergeFunctor op{
      strings.chars_begin(stream),
      offsets.data<cudf::size_type>(),
      static_cast<char*>(bloomBits.data()),
      input.size(),
      bloomBytes};
  cub::DeviceFor::ForEachN(
      thrust::counting_iterator<int32_t>(0),
      bloomBytes,
      op,
      stream.value());
  return makeSerializedBloomFilterColumn(
      std::move(bloomBits), bloomSize, stream, mr);
}

} // namespace facebook::velox::cudf_velox
