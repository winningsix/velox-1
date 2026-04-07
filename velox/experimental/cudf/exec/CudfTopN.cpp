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
#include "velox/experimental/cudf/CudfQueryConfig.h"
#include "velox/experimental/cudf/exec/CudfTopN.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include <cudf/detail/copy.hpp>
#include <cudf/detail/gather.hpp>
#include <cudf/detail/utilities/stream_pool.hpp>
#include <cudf/merge.hpp>
#include <cudf/sorting.hpp>

namespace facebook::velox::cudf_velox {
CudfTopN::CudfTopN(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::TopNNode>& topNNode)
    : exec::Operator(
          driverCtx,
          topNNode->outputType(),
          operatorId,
          topNNode->id(),
          "CudfTopN"),
      NvtxHelper(
          nvtx3::rgb{175, 238, 238}, // Pale Turquoise
          operatorId,
          fmt::format("[{}]", topNNode->id())),
      count_(topNNode->count()),
      topNNode_(topNNode) {
  kBatchSize_ = driverCtx->queryConfig().get<int32_t>(
      CudfQueryConfig::kCudfTopNBatchSize, kBatchSize_);
  const auto numColumns{outputType_->children().size()};
  const auto numSortingKeys{topNNode->sortingKeys().size()};
  std::vector<bool> isSortingKey(numColumns);
  sortKeys_.reserve(numSortingKeys);
  columnOrder_.reserve(numSortingKeys);
  nullOrder_.reserve(numSortingKeys);

  for (int i = 0; i < numSortingKeys; ++i) {
    const auto channel =
        exec::exprToChannel(topNNode->sortingKeys()[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "TopN doesn't allow constant sorting keys");
    sortKeys_.push_back(channel);
    isSortingKey[channel] = true;
    auto const& sortingOrder = topNNode->sortingOrders()[i];
    columnOrder_.push_back(
        sortingOrder.isAscending() ? cudf::order::ASCENDING
                                   : cudf::order::DESCENDING);
    nullOrder_.push_back(
        (sortingOrder.isNullsFirst() ^ !sortingOrder.isAscending())
            ? cudf::null_order::BEFORE
            : cudf::null_order::AFTER);
  }
}

CudfVectorPtr CudfTopN::mergeTopK(
    std::vector<CudfVectorPtr> topNBatches,
    int32_t k,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr,
    bool doSync) {
  std::vector<cudf::table_view> tableViews;
  std::vector<rmm::cuda_stream_view> inputStreams;
  for (const auto& batch : topNBatches) {
    tableViews.push_back(batch->getTableView());
    inputStreams.push_back(batch->stream());
  }
  cudf::detail::join_streams(inputStreams, stream);
  auto mergedTable =
      cudf::merge(tableViews, sortKeys_, columnOrder_, nullOrder_, stream, mr);
  // slice it
  auto topk =
      cudf::split(
          mergedTable->view(), {std::min(k, mergedTable->num_rows())}, stream)
          .front();
  auto const size = topk.num_rows();
  auto resultTable = std::make_unique<cudf::table>(topk, stream, mr);

  // cudf::merge and the table copy above are async on `stream` and read from
  // the input batches' device buffers, which live on their original streams.
  // The caller destroys the input batches after we return, freeing those
  // buffers via cudaFreeAsync on the original streams.  Without sync the frees
  // can race ahead of the reads on `stream`.
  //
  // When doSync=false the caller must keep the input batches alive in
  // pendingReleaseBatches_ until the CUDA stream callback fires.
  if (doSync) {
    stream.synchronize();
  }

  return std::make_shared<CudfVector>(
      topNBatches[0]->pool(),
      outputType_,
      size,
      std::move(resultTable),
      stream);
}

std::unique_ptr<cudf::table> CudfTopN::getTopK(
    cudf::table_view const& values,
    int32_t k,
    rmm::cuda_stream_view stream,
    rmm::device_async_resource_ref mr) {
  auto keys = values.select(sortKeys_);
  auto const indices =
      cudf::stable_sorted_order(keys, columnOrder_, nullOrder_, stream, mr);
  auto const kIndices =
      cudf::split(indices->view(), {std::min(k, indices->size())}, stream)
          .front();
  return cudf::detail::gather(
      values,
      kIndices,
      cudf::out_of_bounds_policy::DONT_CHECK,
      cudf::detail::negative_index_policy::NOT_ALLOWED,
      stream,
      mr);
}

// helper to get topk of a table
CudfVectorPtr CudfTopN::getTopKBatch(CudfVectorPtr cudfInput, int32_t k) {
  if (k == 0 || cudfInput->size() == 0) {
    return nullptr;
  }
  auto stream = cudfInput->stream();
  auto mr = cudf::get_current_device_resource_ref();
  auto values = cudfInput->getTableView();
  auto result = getTopK(values, k, stream, mr);
  auto const size = result->num_rows();
  return std::make_shared<CudfVector>(
      cudfInput->pool(), cudfInput->type(), size, std::move(result), stream);
}

void CudfTopN::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  // Ensure the driver does not feed input while async stream work is in flight.
  // needsInput() already returns false when streamPending_, so this is a
  // belt-and-suspenders guard.
  VELOX_CHECK(!streamPending_, "addInput called while CUDA stream is pending");

  if (count_ == 0 || input->size() == 0) {
    return;
  }

  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput);

  // Enqueue per-batch top-K sort on the input's own CUDA stream (non-blocking).
  topNBatches_.push_back(getTopKBatch(cudfInput, count_));

  auto totalSize = std::accumulate(
      topNBatches_.begin(),
      topNBatches_.end(),
      0,
      [](int32_t sum, const auto& batch) {
        return sum + (batch ? batch->size() : 0);
      });

  if (topNBatches_.size() >= static_cast<size_t>(kBatchSize_) &&
      totalSize >= count_) {
    // Intermediate compaction: merge the accumulated batches down to one.
    // We do this without stream.synchronize() and instead keep the input
    // batches alive in pendingReleaseBatches_ until the CUDA callback fires.
    auto stream = cudfGlobalStreamPool().get_stream();
    auto mr = cudf::get_current_device_resource_ref();

    pendingReleaseBatches_ = std::move(topNBatches_);
    auto merged =
        mergeTopK(pendingReleaseBatches_, count_, stream, mr, /*doSync=*/false);
    topNBatches_.push_back(std::move(merged));

    streamPending_ = true;
    streamDone_ = false;
    streamPromise_ = ContinuePromise("CudfTopN::addInput");

    auto* self = this;
    cudaLaunchHostFunc(
        stream.value(),
        [](void* p) {
          auto* op = static_cast<CudfTopN*>(p);
          // GPU merge complete: release input device buffers and wake driver.
          op->pendingReleaseBatches_.clear();
          op->streamDone_ = true;
          op->streamPending_ = false;
          op->streamPromise_.setValue();
        },
        self);
  }
}

RowVectorPtr CudfTopN::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  // (A) Async work completed — return the buffered result.
  if (streamDone_) {
    streamDone_ = false;
    // topNBatches_ was already moved to pendingReleaseBatches_ before submit;
    // the callback has cleared pendingReleaseBatches_, so nothing to do here.
    finished_ = noMoreInput_;
    return std::move(pendingOutput_);
  }

  // (B) Work is in-flight — driver will park via isBlocked().
  if (streamPending_) {
    return nullptr;
  }

  // (C) Not ready: still receiving input or no batches to merge.
  if (!noMoreInput_) {
    return nullptr;
  }
  if (topNBatches_.empty()) {
    finished_ = true;
    return nullptr;
  }

  // (D) All input received; submit the final merge asynchronously.
  //
  // Single-batch shortcut: the one batch is already top-K sorted; no merge
  // needed, so we avoid the overhead of a no-op merge + async callback.
  if (topNBatches_.size() == 1) {
    auto result = std::move(topNBatches_[0]);
    topNBatches_.clear();
    finished_ = true;
    return result;
  }

  auto stream = topNBatches_[0]->stream();
  auto mr = cudf::get_current_device_resource_ref();

  // Move input batches to pending-release so they stay alive on the heap
  // until the CUDA stream callback confirms the merge is finished.
  pendingReleaseBatches_ = std::move(topNBatches_);
  pendingOutput_ =
      mergeTopK(pendingReleaseBatches_, count_, stream, mr, /*doSync=*/false);

  streamPending_ = true;
  streamDone_ = false;
  streamPromise_ = ContinuePromise("CudfTopN::getOutput");

  auto* self = this;
  cudaLaunchHostFunc(
      stream.value(),
      [](void* p) {
        auto* op = static_cast<CudfTopN*>(p);
        op->pendingReleaseBatches_.clear();
        op->streamDone_ = true;
        op->streamPending_ = false;
        op->streamPromise_.setValue();
      },
      self);

  return nullptr; // driver parks; woken by the callback above
}

void CudfTopN::noMoreInput() {
  Operator::noMoreInput();
  if (topNBatches_.empty()) {
    finished_ = true;
    return;
  }
}

exec::BlockingReason CudfTopN::isBlocked(ContinueFuture* future) {
  if (streamPending_) {
    *future = streamPromise_.getSemiFuture();
    return exec::BlockingReason::kWaitForStream;
  }
  return exec::BlockingReason::kNotBlocked;
}

bool CudfTopN::isFinished() {
  return finished_;
}
} // namespace facebook::velox::cudf_velox
