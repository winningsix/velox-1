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
#include "velox/experimental/ucx-exchange/UcxPartitionedOutput.h"
#include <fmt/format.h>
#include <algorithm>
#include <cstdlib>
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include <cudf/binaryop.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/copying.hpp>
#include <cudf/detail/utilities/stream_pool.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/scalar/scalar.hpp>

using namespace facebook::velox::cudf_velox;
using facebook::velox::exec::Task;
namespace facebook::velox::ucx_exchange {

namespace {
int64_t targetRowsPerUcxChunk(const core::QueryConfig& queryConfig) {
  if (const char* value =
          std::getenv("GLUTEN_UCX_PARTITIONED_OUTPUT_BATCH_ROWS")) {
    try {
      const auto parsed = static_cast<int64_t>(std::stoll(value));
      if (parsed > 0) {
        return parsed;
      }
    } catch (...) {
    }
  }
  return queryConfig.ucxPartitionedOutputBatchRows();
}
} // namespace

// Computes a mapping from names in n2 to names in n1
// and returns that mapping in remap.
// Names in n2 must occurs in n1.
static void getRemapping(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType,
    std::vector<uint32_t>& remap) {
  remap.clear();
  remap.reserve(outputType->size());
  std::unordered_map<std::string, size_t> nextOccurrence;
  for (uint32_t out = 0; out < outputType->size(); ++out) {
    const auto& name = outputType->nameOf(out);
    std::vector<uint32_t> matches;
    for (uint32_t in = 0; in < inputType->size(); ++in) {
      if (inputType->nameOf(in) == name &&
          inputType->childAt(in)->equivalent(*outputType->childAt(out))) {
        matches.push_back(in);
      }
    }
    VELOX_CHECK(
        !matches.empty(),
        "UCX output field {}:{} has no name-and-type match in input {}",
        name,
        outputType->childAt(out)->toString(),
        inputType->toString());
    auto& occurrence = nextOccurrence[name];
    const auto selected = matches[std::min(occurrence, matches.size() - 1)];
    ++occurrence;
    remap.push_back(selected);
  }
}

UcxPartitionedOutput::UcxPartitionedOutput(
    int32_t operatorId,
    exec::DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
    bool eagerFlush)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "cudfPartitionedOutput"),
      NvtxHelper(
          nvtx3::rgb{255, 215, 0}, // Gold
          operatorId,
          fmt::format("[{}]", planNode->id())),
      queueManager_(UcxOutputQueueManager::getInstanceRef()),
      numPartitions_(planNode->numPartitions()),
      isBroadcast_(planNode->isBroadcast()),
      pipelineId_(ctx->pipelineId),
      driverId_(ctx->driverId),
      targetRowsPerChunk_(targetRowsPerUcxChunk(ctx->queryConfig())) {
  if (driverId_ == 0) {
    const auto numDrivers = ctx->task->numOutputDrivers();
    sharedQueueManager()->initializeTask(
        ctx->task,
        planNode->kind(),
        static_cast<int>(numPartitions_),
        numDrivers);
    if (isBroadcast_) {
      sharedQueueManager()->updateOutputBuffers(
          ctx->task->taskId(), static_cast<int>(numPartitions_), true);
    }
    LOG(INFO) << "[UCX-POUT] initialized task=" << ctx->task->taskId()
              << " planNode=" << planNode->id()
              << " destinations=" << numPartitions_
              << " drivers=" << numDrivers
              << " kind=" << core::PartitionedOutputNode::toName(planNode->kind())
              << " targetRowsPerChunk=" << targetRowsPerChunk_;
    VLOG(2) << "UcxPartitionedOutput initialized queue task="
            << ctx->task->taskId() << " destinations=" << numPartitions_
            << " drivers=" << numDrivers
            << " kind=" << core::PartitionedOutputNode::toName(planNode->kind())
            << " targetRowsPerChunk=" << targetRowsPerChunk_;
  }
  this->initPartitionKeys(planNode);
  auto sources = planNode->sources();
  std::vector<std::string> inNames, outNames;
  inNames.reserve(planNode->inputType()->size());
  for (int i = 0; i < planNode->inputType()->size(); ++i) {
    inNames.push_back(planNode->inputType()->nameOf(i));
  }
  outNames.reserve(planNode->outputType()->size());
  for (int i = 0; i < planNode->outputType()->size(); ++i) {
    outNames.push_back(planNode->outputType()->nameOf(i));
  }
  if (inNames != outNames) {
    getRemapping(planNode->inputType(), planNode->outputType(), remap_);
  }
  const auto dropsOnlyFirstColumn =
      planNode->inputType()->size() == planNode->outputType()->size() + 1 &&
      remap_.size() == planNode->outputType()->size() &&
      std::all_of(
          remap_.begin(),
          remap_.end(),
          [next = uint32_t{1}](uint32_t inputIndex) mutable {
            return inputIndex == next++;
          });
  usesPrecomputedPartitionHash_ =
      partitionKeyIndices_.size() == 1 && partitionKeyIndices_.front() == 0 &&
      planNode->inputType()->childAt(0)->kind() == TypeKind::INTEGER &&
      dropsOnlyFirstColumn;
  VLOG(1) << "UcxPartitionedOutput partition input task=" << taskId()
          << " precomputedHash=" << usesPrecomputedPartitionHash_
          << " inputType=" << planNode->inputType()->toString()
          << " outputType=" << planNode->outputType()->toString();
}

void UcxPartitionedOutput::addInput(RowVectorPtr input) {
  CudaAllocationTraceScope allocationTrace(
      fmt::format("UcxPartitionedOutput task={} method=addInput", taskId()));
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_
          << " addInput";
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  auto cudfVector = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfVector, "Input must be a CudfVector");
  VELOX_CHECK(
      !future_.valid() || future_.hasValue(),
      "addInput with outstanding future!");
  VELOX_CHECK(!hasActiveFlush(), "addInput while a flush is still active");

  // Record stats per-input (before buffering).
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
  }

  pendingRows_ += cudfVector->getTableView().num_rows();
  pendingInputs_.push_back(std::move(cudfVector));

  if (targetRowsPerChunk_ <= 0 || pendingRows_ >= targetRowsPerChunk_) {
    flushPending();
  }
}

void UcxPartitionedOutput::flushPending() {
  CudaAllocationTraceScope allocationTrace(
      fmt::format(
          "UcxPartitionedOutput task={} method=flushPending", taskId()));
  if (!hasActiveFlush() && pendingInputs_.empty()) {
    return;
  }

  try {
    if (!hasActiveFlush()) {
      preparePendingFlush();
    }
    advanceActiveFlush();

  } catch (const rmm::bad_alloc& e) {
    VLOG(1)
        << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_
        << " caught memory alloc error, removing all memory in output queues";
    pendingInputs_.clear();
    pendingRows_ = 0;
    clearActiveFlush();
    for (int i = 0; i < numPartitions_; i++) {
      sharedQueueManager()->deleteResults(this->taskId(), i);
    }
    throw;
  }
}

void UcxPartitionedOutput::preparePendingFlush() {
  VELOX_CHECK(!hasActiveFlush());
  VELOX_CHECK(!pendingInputs_.empty());

  activeInputs_ = std::move(pendingInputs_);
  pendingInputs_.clear();
  pendingRows_ = 0;

  auto stream = activeInputs_.back()->stream();
  if (activeInputs_.size() > 1) {
    std::vector<cudf::table_view> views;
    std::vector<rmm::cuda_stream_view> inputStreams;
    views.reserve(activeInputs_.size());
    inputStreams.reserve(activeInputs_.size());
    for (auto& input : activeInputs_) {
      inputStreams.push_back(input->stream());
      views.push_back(input->getTableView());
    }

    cudf::detail::join_streams(inputStreams, stream);
    activeMergedTable_ = cudf::concatenate(
        views, stream, cudf::get_current_device_resource_ref());
    orderCudfVectorDeallocationsAfterStream(
        activeInputs_, inputStreams, stream);
    activeInputs_.clear();
  }

  activeStream_ = stream;
  activeNextRow_ = 0;
  const auto tableRows = activeTableView().num_rows();
  activeRowsPerWindow_ = std::max<cudf::size_type>(
      1,
      targetRowsPerChunk_ > 0
          ? std::min<cudf::size_type>(
                tableRows,
                static_cast<cudf::size_type>(targetRowsPerChunk_))
          : tableRows);
  VLOG(1) << "[UCX-POUT] prepared bounded flush task=" << taskId()
          << " pipeline=" << pipelineId_ << " driver=" << driverId_
          << " rows=" << tableRows
          << " rowsPerWindow=" << activeRowsPerWindow_
          << " destinations=" << numPartitions_
          << " broadcast=" << isBroadcast_;
}

bool UcxPartitionedOutput::hasActiveFlush() const {
  return activeMergedTable_ != nullptr || !activeInputs_.empty();
}

cudf::table_view UcxPartitionedOutput::activeTableView() {
  VELOX_CHECK(hasActiveFlush());
  if (activeMergedTable_) {
    return activeMergedTable_->view();
  }
  VELOX_CHECK_EQ(activeInputs_.size(), 1);
  return activeInputs_.front()->getTableView();
}

void UcxPartitionedOutput::clearActiveFlush() {
  activeInputs_.clear();
  activeMergedTable_.reset();
  activeStream_.reset();
  activeNextRow_ = 0;
  activeRowsPerWindow_ = 0;
}

void UcxPartitionedOutput::updateBackpressure() {
  auto blocked = sharedQueueManager()->checkBlocked(this->taskId(), &future_);
  if (blocked) {
    const auto remainingRows = hasActiveFlush()
        ? activeTableView().num_rows() - activeNextRow_
        : cudf::size_type{0};
    VLOG(1) << "[UCX-POUT] blocked after bounded window task=" << taskId()
            << " pipeline=" << pipelineId_ << " driver=" << driverId_
            << " remainingRows=" << remainingRows
            << " rowsPerWindow=" << activeRowsPerWindow_
            << " destinations=" << numPartitions_;
  }
  blockingReason_ = blocked ? exec::BlockingReason::kWaitForConsumer
                            : exec::BlockingReason::kNotBlocked;
}

void UcxPartitionedOutput::advanceActiveFlush() {
  VELOX_CHECK(hasActiveFlush());
  VELOX_CHECK(activeStream_.has_value());
  VELOX_CHECK_EQ(blockingReason_, exec::BlockingReason::kNotBlocked);

  auto tableView = activeTableView();
  const auto tableRows = tableView.num_rows();
  if (activeNextRow_ >= tableRows) {
    clearActiveFlush();
    return;
  }

  auto stream = *activeStream_;
  const auto end = std::min<cudf::size_type>(
      tableRows, activeNextRow_ + activeRowsPerWindow_);
  auto slices = cudf::slice(tableView, {activeNextRow_, end}, stream);
  VELOX_CHECK_EQ(slices.size(), 1);
  auto window = slices[0];

  if (isBroadcast_ || numPartitions_ == 1) {
    auto outputView = remap_.empty()
        ? window
        : window.select(remap_.begin(), remap_.end());
    auto packedCols = cudf::pack(
        outputView, stream, cudf::get_current_device_resource_ref());
    stream.synchronize();
    auto packedColsPtr = std::make_unique<cudf::packed_columns>(
        std::move(packedCols.metadata), std::move(packedCols.gpu_data));
    // Broadcast queues fan out a destination-zero enqueue to all consumers.
    sharedQueueManager()->enqueue(
        this->taskId(),
        0,
        std::move(packedColsPtr),
        outputView.num_rows());
  } else if (partitionKeyIndices_.size() > 0 || spec_ == "gather") {
    hashPartition(window, stream);
  } else {
    equalPartition(window, stream);
  }

  activeNextRow_ = end;
  if (activeNextRow_ == tableRows) {
    clearActiveFlush();
  }
  updateBackpressure();
}

exec::BlockingReason UcxPartitionedOutput::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != exec::BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = exec::BlockingReason::kNotBlocked;
    return exec::BlockingReason::kWaitForConsumer;
  }
  return exec::BlockingReason::kNotBlocked;
}

RowVectorPtr UcxPartitionedOutput::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (finished_) {
    return nullptr;
  }
  // The Driver normally calls isBlocked() first. Keep this guard so an
  // outstanding queue future is never overwritten by a direct getOutput().
  if (blockingReason_ != exec::BlockingReason::kNotBlocked) {
    return nullptr;
  }
  if (hasActiveFlush() || (noMoreInput_ && !pendingInputs_.empty())) {
    flushPending();
  }
  // A final window may have filled the queue. Publish EOS only after the Driver
  // has observed and resumed that queue future.
  if (noMoreInput_ && !hasActiveFlush() && pendingInputs_.empty() &&
      blockingReason_ == exec::BlockingReason::kNotBlocked) {
    LOG(INFO) << "[UCX-POUT] noMoreInput task=" << taskId()
              << " pipeline=" << pipelineId_
              << " driver=" << driverId_
              << " pendingRows=" << pendingRows_
              << " destinations=" << numPartitions_;
    sharedQueueManager()->noMoreData(this->taskId());
    LOG(INFO) << "[UCX-POUT] noMoreData sent task=" << taskId()
              << " pipeline=" << pipelineId_
              << " driver=" << driverId_;
    finished_ = true;
  }
  return nullptr;
}

bool UcxPartitionedOutput::isFinished() {
  return finished_;
}

std::shared_ptr<facebook::velox::ucx_exchange::UcxOutputQueueManager>
UcxPartitionedOutput::sharedQueueManager() {
  auto shared_queueManager = queueManager_.lock();
  VELOX_CHECK_NOT_NULL(
      shared_queueManager, "OutputQueueManager was already destructed");
  return shared_queueManager;
}

void UcxPartitionedOutput::initPartitionKeys(
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode) {
  // Following Logic copied direcly from CudLocalPartition (!)

  // Following is IMO a hacky way to get the partition key indices. It is to
  // workaround the fact that the partition spec constructs the hash function
  // directly and has no public methods to get the partition key indices.

  // When the operator is of type kRepartition, the partition spec is a string
  // in the format "HASH(key1, key2, ...)"
  // We're going to extract the keys between HASH( and ) and find their indices
  // in the output row type.

  // When operator is of type kGather, we don't need to store any partition key
  // indices because we're going to merge all the incoming streams together.

  // Get partition function specification string
  spec_ = planNode->partitionFunctionSpec().toString();
  VLOG(1) << "UcxPartitionedOutput initPartitionKeys task=" << taskId()
          << " node=" << planNode->id() << " spec=" << spec_
          << " inputType=" << planNode->inputType()->toString()
          << " outputType=" << planNode->outputType()->toString();

  // Only parse keys if it's a hash function
  if (spec_.find("HASH(") != std::string::npos) {
    // Extract keys between HASH( and )
    size_t start = spec_.find("HASH(") + 5;
    size_t end = spec_.find(")", start);
    if (start != std::string::npos && end != std::string::npos) {
      std::string keysStr = spec_.substr(start, end - start);

      // Split by comma to get individual keys.
      std::vector<std::string> keys;
      size_t pos = 0;
      while ((pos = keysStr.find(",")) != std::string::npos) {
        std::string key = keysStr.substr(0, pos);
        keys.push_back(key);
        keysStr.erase(0, pos + 1);
      }
      keys.push_back(keysStr); // Add the last key.

      // Find field indices for each key.
      const auto& rowType = planNode->inputType();
      for (const auto& key : keys) {
        auto trimmedKey = key;
        // Trim whitespace
        trimmedKey.erase(0, trimmedKey.find_first_not_of(" "));
        trimmedKey.erase(trimmedKey.find_last_not_of(" ") + 1);

        auto fieldIndex = rowType->getChildIdx(trimmedKey);
        partitionKeyIndices_.push_back(fieldIndex);
        VLOG(1) << "UcxPartitionedOutput partition key task=" << taskId()
                << " key=" << trimmedKey << " inputIndex=" << fieldIndex;
      }
    }
  }
}

void UcxPartitionedOutput::hashPartition(
    cudf::table_view tableView,
    rmm::cuda_stream_view stream) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_
          << " Hashing and partitioning into " << numPartitions_ << " chunks";

  // Use cudf hash partitioning
  std::vector<cudf::size_type> partitionKeyIndices;
  for (const auto& idx : partitionKeyIndices_) {
    partitionKeyIndices.push_back(static_cast<cudf::size_type>(idx));
  }

  auto [partitionedTable, partitionOffsets] = [&]() {
    if (usesPrecomputedPartitionHash_) {
      const auto hashColumn = tableView.column(partitionKeyIndices.front());
      VELOX_CHECK(
          hashColumn.type().id() == cudf::type_id::INT32,
          "Precomputed Spark partition hash must be INT32");
      VELOX_CHECK_EQ(
          hashColumn.null_count(),
          0,
          "Precomputed Spark partition hash must be non-null");
      cudf::numeric_scalar<int32_t> divisor(
          static_cast<int32_t>(numPartitions_),
          true,
          stream,
          cudf::get_current_device_resource_ref());
      auto partitionMap = cudf::binary_operation(
          hashColumn,
          divisor,
          cudf::binary_operator::PMOD,
          cudf::data_type{cudf::type_id::INT32},
          stream,
          cudf::get_current_device_resource_ref());
      return cudf::partition(
          tableView,
          partitionMap->view(),
          numPartitions_,
          stream,
          cudf::get_current_device_resource_ref());
    }
    return cudf::hash_partition(
        tableView,
        partitionKeyIndices,
        numPartitions_,
        cudf::hash_id::HASH_MURMUR3,
        cudf::DEFAULT_HASH_SEED,
        stream);
  }();

  VELOX_CHECK_EQ(partitionOffsets.size(), numPartitions_ + 1);
  VELOX_CHECK_EQ(partitionOffsets[0], 0);

  // Erase first element since it's always 0 and we don't need it.
  partitionOffsets.erase(partitionOffsets.begin());
  partitionOffsets.pop_back();

  splitAndEnqueue(partitionedTable->view(), partitionOffsets, stream);
}

void UcxPartitionedOutput::equalPartition(
    cudf::table_view tableView,
    rmm::cuda_stream_view stream) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_
          << " Splitting into " << numPartitions_ << " chunks";
  std::vector<cudf::size_type> offsets;
  cudf::size_type size = tableView.num_rows();
  for (int i = 1; i < numPartitions_; ++i) {
    cudf::size_type idx = size * i / numPartitions_;
    offsets.push_back(idx);
  }
  splitAndEnqueue(tableView, offsets, stream);
}

void UcxPartitionedOutput::splitAndEnqueue(
    cudf::table_view tableView,
    std::vector<cudf::size_type> offsets,
    rmm::cuda_stream_view stream) {
  auto contiguousTables = cudf::contiguous_split(
      tableView, offsets, stream, cudf::get_current_device_resource_ref());

  // Synchronize the stream to ensure CUDA operations complete before enqueuing.
  // UCXX/UCX is not stream-aware, so without syncing, data could be sent before
  // the GPU kernels have finished writing to the buffers.
  stream.synchronize();

  VELOX_CHECK_EQ(
      offsets.size() + 1, numPartitions_, "mismatch in numPartitions_");
  auto queueManager = sharedQueueManager();
  for (int i = 0; i < numPartitions_; ++i) {
    auto const& partitionTable = contiguousTables[i];
    const auto partitionRows = partitionTable.table.num_rows();
    if (partitionRows == 0) {
      // Skip empty partitions.
      continue;
    }

    const bool rowChunkingNeeded =
        targetRowsPerChunk_ > 0 && partitionRows > targetRowsPerChunk_;
    if (rowChunkingNeeded) {
      cudf::size_type rowsPerChunk = std::min<cudf::size_type>(
          partitionRows, static_cast<cudf::size_type>(targetRowsPerChunk_));
      VLOG(2) << "UcxPartitionedOutput chunking task=" << taskId()
              << " destination=" << i << " rows=" << partitionRows
              << " rowsPerChunk=" << rowsPerChunk
              << " targetRowsPerChunk=" << targetRowsPerChunk_;
      for (cudf::size_type start = 0; start < partitionRows;
           start += rowsPerChunk) {
        const auto end =
            std::min<cudf::size_type>(partitionRows, start + rowsPerChunk);
        auto slicedTables = cudf::slice(partitionTable.table, {start, end});
        VELOX_CHECK_EQ(slicedTables.size(), 1);
        auto outputView = remap_.empty()
            ? slicedTables[0]
            : slicedTables[0].select(remap_.begin(), remap_.end());
        auto packedCols = cudf::pack(outputView, stream);
        stream.synchronize();
        auto packedColsPtr = std::make_unique<cudf::packed_columns>(
            std::move(packedCols.metadata), std::move(packedCols.gpu_data));
        queueManager->enqueue(
            this->taskId(),
            i,
            std::move(packedColsPtr),
            outputView.num_rows());
      }
      continue;
    }

    std::unique_ptr<cudf::packed_columns> packedColsPtr;
    if (remap_.empty()) {
      packedColsPtr = std::make_unique<cudf::packed_columns>(
          std::move(contiguousTables[i].data.metadata),
          std::move(contiguousTables[i].data.gpu_data));
    } else {
      auto outputView =
          partitionTable.table.select(remap_.begin(), remap_.end());
      auto packedCols = cudf::pack(outputView, stream);
      stream.synchronize();
      packedColsPtr = std::make_unique<cudf::packed_columns>(
          std::move(packedCols.metadata), std::move(packedCols.gpu_data));
    }

    // enqueue partition data on Ucx Output Buffer
    queueManager->enqueue(
        this->taskId(),
        i,
        std::move(packedColsPtr),
        partitionTable.table.num_rows());
  }
}

} // namespace facebook::velox::ucx_exchange
