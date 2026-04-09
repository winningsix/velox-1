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

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/CudfHashJoin.h"
#include "velox/experimental/cudf/exec/GpuGuard.h"
#include "velox/experimental/cudf/exec/PinnedHostMemory.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/AstExpression.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h" // NOLINT(misc-unused-headers)
#include "velox/type/TypeUtil.h"

#include <cudf/aggregation.hpp>
#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/copying.hpp>
#include <cudf/filling.hpp>
#include <cudf/groupby.hpp>
#include <cudf/join/filtered_join.hpp>
#include <cudf/join/join.hpp>
#include <cudf/join/mixed_join.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/unary.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>

#include <nvtx3/nvtx3.hpp>

#include <iostream>

namespace facebook::velox::cudf_velox {

void CudfHashJoinProbe::close() {
  auto gpuNs = gpuTimer_.totalNanos();
  if (gpuNs > 0) {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        kGpuComputeNanos,
        RuntimeCounter(gpuNs, RuntimeCounter::Unit::kNanos));
  }
  hashObject_.reset();
  buildBatches_.reset();
  Operator::close();
  filterEvaluator_.reset();
  scalars_.clear();
  tree_ = {};
  accumulatedProbeInputs_.clear();
  accumulatedProbeRows_ = 0;
  accumulatedProbeBytes_ = 0;
}

void CudfHashJoinProbe::buildHashTable() {
  VELOX_CHECK(buildBatches_.has_value());
  VELOX_CHECK(!hashObject_.has_value());
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  auto& hostBatches = buildBatches_.value();
  auto buildType = joinNode_->sources()[1]->outputType();

  if (hostBatches.empty()) {
    std::vector<std::unique_ptr<cudf::column>> emptyCols;
    for (int i = 0; i < buildType->size(); ++i) {
      auto cudfType = cudf::data_type{veloxToCudfTypeId(buildType->childAt(i))};
      emptyCols.push_back(cudf::make_empty_column(cudfType));
    }
    auto emptyTbl = std::make_unique<cudf::table>(std::move(emptyCols));
    std::vector<std::shared_ptr<cudf::table>> emptyTbls;
    emptyTbls.push_back(std::move(emptyTbl));
    std::vector<std::shared_ptr<cudf::hash_join>> emptyHashObjs;
    emptyHashObjs.push_back(nullptr);
    hashObject_ = std::make_pair(
        std::move(emptyTbls), std::move(emptyHashObjs));
    buildBatches_.reset();
    return;
  }

  auto stream = cudfGlobalStreamPool().get_stream();
  buildStream_ = stream;

  size_t totalHostBytes = 0;
  for (auto& hb : hostBatches) {
    totalHostBytes += hb.data->size();
  }
  std::cerr << "GPU_MEM_SNAPSHOT [buildHashTable-pre-H2D] "
               << gpuMemorySnapshotString()
               << " hostBatches=" << hostBatches.size()
               << " totalHostBytes=" << succinctBytes(totalHostBytes)
               << std::endl;

  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinProbe::buildHashTable hostBatches="
            << hostBatches.size();
  }

  // H2D: copy each host-pinned batch back to GPU and reconstruct CudfVectors.
  std::vector<CudfVectorPtr> gpuBatches;
  gpuBatches.reserve(hostBatches.size());
  for (auto& hb : hostBatches) {
    rmm::device_buffer devBuf(hb.data->size(), stream);
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        devBuf.data(),
        hb.data->data(),
        hb.data->size(),
        cudaMemcpyHostToDevice,
        stream.value()));
    auto metadata = std::make_unique<std::vector<uint8_t>>(
        std::move(hb.metadata));
    cudf::packed_columns pc{std::move(metadata),
                            std::make_unique<rmm::device_buffer>(
                                std::move(devBuf))};
    auto unpacked = std::make_unique<cudf::packed_table>(cudf::unpack(pc));
    auto numRows = hb.numRows;
    gpuBatches.push_back(std::make_shared<CudfVector>(
        operatorCtx_->pool(),
        buildType,
        numRows,
        std::move(unpacked),
        stream));
  }
  buildBatches_.reset();
  std::cerr << "GPU_MEM_SNAPSHOT [buildHashTable-post-H2D] "
               << gpuMemorySnapshotString()
               << " gpuBatches=" << gpuBatches.size()
               << " (all build data now on GPU, about to concatenate)"
               << std::endl;

  auto tbls = getConcatenatedTableBatched(gpuBatches, buildType, stream);
  // Release individual GPU batches immediately — the concatenated tables in
  // `tbls` own all the data now. Without this, both gpuBatches (~N bytes)
  // and tbls (~N bytes) coexist on GPU during hash_join construction,
  // doubling peak memory vs v4 which cleared inputs_ here.
  gpuBatches.clear();

  for (auto const& tbl : tbls) {
    VELOX_CHECK_NOT_NULL(tbl);
  }

  auto rightKeys = joinNode_->rightKeys();
  auto buildKeyIndices = std::vector<cudf::size_type>(rightKeys.size());
  for (size_t i = 0; i < buildKeyIndices.size(); i++) {
    buildKeyIndices[i] = static_cast<cudf::size_type>(
        buildType->getChildIdx(rightKeys[i]->name()));
  }

  bool needsHashJoin =
      (joinNode_->isInnerJoin() || joinNode_->isLeftJoin() ||
       joinNode_->isRightJoin());

  std::vector<std::shared_ptr<cudf::hash_join>> hashObjects;
  for (size_t i = 0; i < tbls.size(); i++) {
    hashObjects.push_back(
        needsHashJoin ? std::make_shared<cudf::hash_join>(
                            tbls[i]->view().select(buildKeyIndices),
                            cudf::null_equality::UNEQUAL,
                            stream)
                      : nullptr);
  }

  std::vector<std::shared_ptr<cudf::table>> shared_tbls;
  for (auto& tbl : tbls) {
    shared_tbls.push_back(std::move(tbl));
  }

  hashObject_ = std::make_pair(
      std::move(shared_tbls), std::move(hashObjects));
  std::cerr << "GPU_MEM_SNAPSHOT [buildHashTable-post-build] "
               << gpuMemorySnapshotString()
               << " (hash table built, concatenated tables + hash objects on GPU)"
               << std::endl;

  // Initialize right-join matched flags under the same GpuGuard.
  if (joinNode_->isRightJoin()) {
    auto& rightTables = hashObject_.value().first;
    rightMatchedFlags_.clear();
    rightMatchedFlags_.reserve(rightTables.size());
    for (auto& rt : rightTables) {
      auto n = rt->num_rows();
      auto false_scalar = cudf::numeric_scalar<bool>(false, true, stream);
      auto flags_col = cudf::make_column_from_scalar(
          false_scalar, n, stream, cudf::get_current_device_resource_ref());
      rightMatchedFlags_.push_back(std::move(flags_col));
    }
  }
}

void CudfHashJoinBridge::setBuildBatches(
    std::vector<HostBuildBatch> batches) {
  VLOG_IF(2, CudfConfig::getInstance().debugEnabled)
      << "CudfHashJoinBridge::setBuildBatches  batches=" << batches.size();
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(
        !buildBatches_.has_value(),
        "setBuildBatches may be called only once");
    buildBatches_ = std::move(batches);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<std::vector<HostBuildBatch>>
CudfHashJoinBridge::buildBatchesOrFuture(ContinueFuture* future) {
  VLOG_IF(2, CudfConfig::getInstance().debugEnabled)
      << "CudfHashJoinBridge::buildBatchesOrFuture";
  std::lock_guard<std::mutex> l(mutex_);
  if (buildBatches_.has_value()) {
    return std::move(buildBatches_);
  }
  promises_.emplace_back("CudfHashJoinBridge::buildBatchesOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

CudfHashJoinBuild::CudfHashJoinBuild(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    // TODO check outputType should be set or not?
    : exec::Operator(
          driverCtx,
          nullptr, // joinNode->sources(),
          operatorId,
          joinNode->id(),
          "CudfHashJoinBuild"),
      NvtxHelper(
          nvtx3::rgb{65, 105, 225}, // Royal Blue
          operatorId,
          fmt::format("[{}]", joinNode->id())),
      joinNode_(joinNode) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "CudfHashJoinBuild constructor";
  }
}

void CudfHashJoinBuild::addInput(RowVectorPtr input) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBuild::addInput";
  }
  if (input->size() == 0) {
    endGpuRegion();
    return;
  }
  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput);

  auto stream = cudfInput->stream();

  // D2H: pack into contiguous device buffer, copy to pinned host memory.
  // Runs under the GPU region started by the source operator (scan).
  auto packed = cudf::pack(cudfInput->getTableView(), stream);
  auto devSize = packed.gpu_data->size();
  auto hostBuf = std::make_shared<PinnedHostBuffer>(devSize);
  CUDF_CUDA_TRY(cudaMemcpyAsync(
      hostBuf->data(),
      packed.gpu_data->data(),
      devSize,
      cudaMemcpyDeviceToHost,
      stream.value()));
  stream.synchronize();

  auto numRows = static_cast<vector_size_t>(cudfInput->size());
  hostInputs_.push_back(
      {std::move(*packed.metadata), std::move(hostBuf), numRows});

  // D2H complete — end the GPU region started by the source operator.
  // All GPU data for this batch is now on host; semaphore released.
  endGpuRegion();
}

bool CudfHashJoinBuild::needsInput() const {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBuild::needsInput";
  }
  return !noMoreInput_;
}

RowVectorPtr CudfHashJoinBuild::getOutput() {
  return nullptr;
}

void CudfHashJoinBuild::noMoreInput() {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBuild::noMoreInput";
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }
  // Collect host-resident batches from all peers (pointer moves, no GPU work).
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* build = dynamic_cast<CudfHashJoinBuild*>(op);
    VELOX_CHECK_NOT_NULL(build);
    hostInputs_.insert(
        hostInputs_.end(),
        std::make_move_iterator(build->hostInputs_.begin()),
        std::make_move_iterator(build->hostInputs_.end()));
  }

  SCOPE_EXIT {
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  };

  // Pass host-pinned batches to bridge — probe side does H2D + hash table
  // build lazily under its own GpuGuard. Build pipeline never holds the
  // GPU semaphore beyond the brief pack+D2H in addInput().
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  auto cudfHashJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  cudfHashJoinBridge->setBuildBatches(std::move(hostInputs_));
}

exec::BlockingReason CudfHashJoinBuild::isBlocked(ContinueFuture* future) {
  if (!future_.valid()) {
    return exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return exec::BlockingReason::kWaitForJoinBuild;
}

bool CudfHashJoinBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

CudfHashJoinProbe::CudfHashJoinProbe(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    : exec::Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "CudfHashJoinProbe"),
      NvtxHelper(
          nvtx3::rgb{0, 128, 128}, // Teal
          operatorId,
          fmt::format("[{}]", joinNode->id())),
      joinNode_(joinNode),
      cudaEvent_(std::make_unique<CudaEvent>(cudaEventDisableTiming)) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "CudfHashJoinProbe constructor";
  }
  auto probeType = joinNode_->sources()[0]->outputType();
  auto buildType = joinNode_->sources()[1]->outputType();
  auto const& leftKeys = joinNode_->leftKeys(); // probe keys
  auto const& rightKeys = joinNode_->rightKeys(); // build keys

  if (CudfConfig::getInstance().debugEnabled) {
    for (int i = 0; i < probeType->names().size(); i++) {
      VLOG(1) << "Left column " << i << ": " << probeType->names()[i];
    }

    for (int i = 0; i < buildType->names().size(); i++) {
      VLOG(1) << "Right column " << i << ": " << buildType->names()[i];
    }

    for (int i = 0; i < leftKeys.size(); i++) {
      VLOG(1) << "Left key " << i << ": " << leftKeys[i]->name() << " "
              << leftKeys[i]->type()->kind();
    }

    for (int i = 0; i < rightKeys.size(); i++) {
      VLOG(1) << "Right key " << i << ": " << rightKeys[i]->name() << " "
              << rightKeys[i]->type()->kind();
    }
  }

  auto const probeTableNumColumns = probeType->size();
  leftKeyIndices_ = std::vector<cudf::size_type>(leftKeys.size());
  for (size_t i = 0; i < leftKeyIndices_.size(); i++) {
    leftKeyIndices_[i] = static_cast<cudf::size_type>(
        probeType->getChildIdx(leftKeys[i]->name()));
    VELOX_CHECK_LT(leftKeyIndices_[i], probeTableNumColumns);
  }
  auto const buildTableNumColumns = buildType->size();
  rightKeyIndices_ = std::vector<cudf::size_type>(rightKeys.size());
  for (size_t i = 0; i < rightKeyIndices_.size(); i++) {
    rightKeyIndices_[i] = static_cast<cudf::size_type>(
        buildType->getChildIdx(rightKeys[i]->name()));
    VELOX_CHECK_LT(rightKeyIndices_[i], buildTableNumColumns);
  }

  auto outputType = joinNode_->outputType();
  leftColumnIndicesToGather_ = std::vector<cudf::size_type>();
  rightColumnIndicesToGather_ = std::vector<cudf::size_type>();
  leftColumnOutputIndices_ = std::vector<size_t>();
  rightColumnOutputIndices_ = std::vector<size_t>();
  for (int i = 0; i < outputType->names().size(); i++) {
    auto const outputName = outputType->names()[i];
    if (CudfConfig::getInstance().debugEnabled) {
      VLOG(1) << "Output column " << i << ": " << outputName;
    }
    auto channel = probeType->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      leftColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      leftColumnOutputIndices_.push_back(i);
      continue;
    }
    channel = buildType->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      rightColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      rightColumnOutputIndices_.push_back(i);
      continue;
    }
    VELOX_FAIL(
        "Join field {} not in probe or build input", outputType->children()[i]);
  }

  if (CudfConfig::getInstance().debugEnabled) {
    for (int i = 0; i < leftColumnIndicesToGather_.size(); i++) {
      VLOG(1) << "Left index to gather " << i << ": "
              << leftColumnIndicesToGather_[i];
    }

    for (int i = 0; i < rightColumnIndicesToGather_.size(); i++) {
      VLOG(1) << "Right index to gather " << i << ": "
              << rightColumnIndicesToGather_[i];
    }
  }

}

void CudfHashJoinProbe::initialize() {
  Operator::initialize();

  if (!joinNode_->filter()) {
    return;
  }

  auto probeType = joinNode_->sources()[0]->outputType();
  auto buildType = joinNode_->sources()[1]->outputType();

  exec::ExprSet exprs({joinNode_->filter()}, operatorCtx_->execCtx());
  VELOX_CHECK_EQ(exprs.exprs().size(), 1);

  // Create a reusable evaluator for the filter column. This is expensive to
  // build, and the expression + input schema are stable for the lifetime of
  // the operator instance.
  std::vector<velox::RowTypePtr> filterRowTypes{probeType, buildType};
  filterEvaluator_ = createCudfExpression(
      exprs.exprs()[0],
      facebook::velox::type::concatRowTypes(filterRowTypes));

  // We don't need to get tables that contain conditional comparison columns
  // We'll pass the entire table. The ast will handle finding the required
  // columns. This is required because we build the ast with whole row schema
  // and the column locations in that schema translate to column locations
  // in whole tables

  std::vector<PrecomputeInstruction> rightPrecomputeInstructions;
  std::vector<PrecomputeInstruction> leftPrecomputeInstructions;
  static constexpr bool kAllowPureAstOnly = true;
  if (joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin()) {
    createAstTree(
        exprs.exprs()[0],
        tree_,
        scalars_,
        buildType,
        probeType,
        rightPrecomputeInstructions,
        leftPrecomputeInstructions,
        kAllowPureAstOnly);
  } else {
    createAstTree(
        exprs.exprs()[0],
        tree_,
        scalars_,
        probeType,
        buildType,
        leftPrecomputeInstructions,
        rightPrecomputeInstructions,
        kAllowPureAstOnly);
  }
  if (leftPrecomputeInstructions.size() > 0 ||
      rightPrecomputeInstructions.size() > 0) {
    VELOX_NYI("Filters that require precomputation are not yet supported");
  }
}

bool CudfHashJoinProbe::needsInput() const {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinProbe::needsInput";
  }
  if (joinNode_->isRightSemiFilterJoin()) {
    return !noMoreInput_;
  }
  auto targetBytes = CudfConfig::getInstance().gpuTargetBatchBytes;
  auto minRows = CudfConfig::getInstance().gpuTargetBatchRows;
  bool belowThreshold;
  if (accumulatedProbeInputs_.empty()) {
    belowThreshold = true;
  } else if (targetBytes > 0 && minRows > 0) {
    belowThreshold =
        accumulatedProbeBytes_ < targetBytes &&
        accumulatedProbeRows_ < minRows;
  } else if (targetBytes > 0) {
    belowThreshold = accumulatedProbeBytes_ < targetBytes;
  } else if (minRows > 0) {
    belowThreshold = accumulatedProbeRows_ < minRows;
  } else {
    belowThreshold = false;
  }
  return !noMoreInput_ && !finished_ && input_ == nullptr && belowThreshold;
}

void CudfHashJoinProbe::addInput(RowVectorPtr input) {
  if (skipInput_) {
    VELOX_CHECK_NULL(input_);
    return;
  }
  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput);
  if (joinNode_->isRightSemiFilterJoin()) {
    if (input->size() > 0) {
      inputs_.push_back(std::move(cudfInput));
    }
    return;
  }

  if (input->size() > 0) {
    accumulatedProbeRows_ += input->size();
    accumulatedProbeBytes_ += cudfInput->estimateFlatSize();
    accumulatedProbeInputs_.push_back(std::move(cudfInput));
  }
}

void CudfHashJoinProbe::noMoreInput() {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinProbe::noMoreInput";
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  GpuGuard gpuGuard;
  Operator::noMoreInput();
  if (!joinNode_->isRightJoin() && !joinNode_->isRightSemiFilterJoin()) {
    return;
  }
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // Only last driver collects all answers
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }

  SCOPE_EXIT {
    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  };

  if (joinNode_->isRightJoin()) {
    isLastDriver_ = true;
    if (hashObject_.has_value()) {
      auto stream = cudfGlobalStreamPool().get_stream();
      // Keep old flag columns alive until after sync: the binary_operation
      // on `stream` reads from them asynchronously, but their memory lives
      // on a different stream. Destroying them before the sync would free
      // the buffers via cudaFreeAsync on the old stream, racing with the
      // reads on `stream`.
      std::vector<std::unique_ptr<cudf::column>> oldFlags;
      for (auto& peer : peers) {
        if (peer.get() == operatorCtx_->driver()) {
          continue;
        }
        auto op = peer->findOperator(planNodeId());
        auto* probe = dynamic_cast<CudfHashJoinProbe*>(op);
        if (probe == nullptr) {
          continue;
        }
        for (size_t p = 0; p < rightMatchedFlags_.size(); ++p) {
          auto or_result = cudf::binary_operation(
              rightMatchedFlags_[p]->view(),
              probe->rightMatchedFlags_[p]->view(),
              cudf::binary_operator::BITWISE_OR,
              cudf::data_type{cudf::type_id::BOOL8},
              stream,
              cudf::get_current_device_resource_ref());
          oldFlags.push_back(std::move(rightMatchedFlags_[p]));
          rightMatchedFlags_[p] = std::move(or_result);
        }
      }
      stream.synchronize();
    }
    return;
  }

  // Handling RightSemiFilterJoin
  // Collect results from peers
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* probe = dynamic_cast<CudfHashJoinProbe*>(op);
    VELOX_CHECK_NOT_NULL(probe);
    inputs_.insert(inputs_.end(), probe->inputs_.begin(), probe->inputs_.end());
  }

  auto stream = cudfGlobalStreamPool().get_stream();
  auto tbl = getConcatenatedTable(
      inputs_, joinNode_->sources()[1]->outputType(), stream);

  VELOX_CHECK_NOT_NULL(tbl);

  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Probe table number of columns: " << tbl->num_columns();
    VLOG(1) << "Probe table number of rows: " << tbl->num_rows();
  }

  // Store the concatenated table in input_
  input_ = std::make_shared<CudfVector>(
      operatorCtx_->pool(),
      joinNode_->outputType(),
      tbl->num_rows(),
      std::move(tbl),
      stream);

  inputs_.clear();
}

std::unique_ptr<cudf::table> CudfHashJoinProbe::unfilteredOutput(
    cudf::table_view leftTableView,
    cudf::column_view leftIndicesCol,
    cudf::table_view rightTableView,
    cudf::column_view rightIndicesCol,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::column>> joinedCols;
  auto leftInput = leftTableView.select(leftColumnIndicesToGather_);
  auto rightInput = rightTableView.select(rightColumnIndicesToGather_);
  auto leftResult = cudf::gather(leftInput, leftIndicesCol, oobPolicy, stream);
  auto rightResult =
      cudf::gather(rightInput, rightIndicesCol, oobPolicy, stream);

  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Left result number of columns: " << leftResult->num_columns();
    VLOG(1) << "Right result number of columns: " << rightResult->num_columns();
  }

  auto leftCols = leftResult->release();
  auto rightCols = rightResult->release();
  joinedCols.resize(outputType_->names().size());
  for (int i = 0; i < leftColumnOutputIndices_.size(); i++) {
    joinedCols[leftColumnOutputIndices_[i]] = std::move(leftCols[i]);
  }
  for (int i = 0; i < rightColumnOutputIndices_.size(); i++) {
    joinedCols[rightColumnOutputIndices_[i]] = std::move(rightCols[i]);
  }
  if (buildStream_.has_value()) {
    // Ensure deallocation of build table happens after probe gathers
    cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
  }
  stream.synchronize();
  return std::make_unique<cudf::table>(std::move(joinedCols));
}

std::unique_ptr<cudf::table> CudfHashJoinProbe::filteredOutput(
    cudf::table_view leftTableView,
    cudf::column_view leftIndicesCol,
    cudf::table_view rightTableView,
    cudf::column_view rightIndicesCol,
    std::function<std::vector<std::unique_ptr<cudf::column>>(
        std::vector<std::unique_ptr<cudf::column>>&&,
        cudf::column_view)> func,
    rmm::cuda_stream_view stream) {
  auto leftResult =
      cudf::gather(leftTableView, leftIndicesCol, oobPolicy, stream);
  auto rightResult =
      cudf::gather(rightTableView, rightIndicesCol, oobPolicy, stream);
  auto leftColsSize = leftResult->num_columns();
  auto rightColsSize = rightResult->num_columns();

  std::vector<std::unique_ptr<cudf::column>> joinedCols = leftResult->release();
  auto rightCols = rightResult->release();
  joinedCols.insert(
      joinedCols.end(),
      std::make_move_iterator(rightCols.begin()),
      std::make_move_iterator(rightCols.end()));

  VELOX_CHECK_NOT_NULL(
      filterEvaluator_,
      "Join filter evaluator must be initialized before filteredOutput()");
  auto filterColumns = filterEvaluator_->eval(
      joinedCols, stream, cudf::get_current_device_resource_ref());
  auto filterColumn = asView(filterColumns);

  joinedCols = func(std::move(joinedCols), filterColumn);

  auto filteredjoinedCols =
      std::vector<std::unique_ptr<cudf::column>>(outputType_->names().size());
  for (int i = 0; i < leftColumnOutputIndices_.size(); i++) {
    filteredjoinedCols[leftColumnOutputIndices_[i]] =
        std::move(joinedCols[leftColumnIndicesToGather_[i]]);
  }
  for (int i = 0; i < rightColumnOutputIndices_.size(); i++) {
    filteredjoinedCols[rightColumnOutputIndices_[i]] =
        std::move(joinedCols[leftColsSize + rightColumnIndicesToGather_[i]]);
  }
  joinedCols = std::move(filteredjoinedCols);
  if (buildStream_.has_value()) {
    // Ensure any deallocation of join indices is ordered wrt probe gathers
    cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
  }
  stream.synchronize();
  return std::make_unique<cudf::table>(std::move(joinedCols));
}

std::unique_ptr<cudf::table> CudfHashJoinProbe::filteredOutputIndices(
    cudf::table_view leftTableView,
    cudf::column_view leftIndicesCol,
    cudf::table_view rightTableView,
    cudf::column_view rightIndicesCol,
    cudf::join_kind joinKind,
    rmm::cuda_stream_view stream) {
  auto [filteredLeftJoinIndices, filteredRightJoinIndices] =
      cudf::filter_join_indices(
          leftTableView,
          rightTableView,
          leftIndicesCol,
          rightIndicesCol,
          tree_.back(),
          joinKind,
          stream);

  auto filteredLeftIndicesSpan =
      cudf::device_span<cudf::size_type const>{*filteredLeftJoinIndices};
  auto filteredRightIndicesSpan =
      cudf::device_span<cudf::size_type const>{*filteredRightJoinIndices};
  auto filteredLeftIndicesCol = cudf::column_view{filteredLeftIndicesSpan};
  auto filteredRightIndicesCol = cudf::column_view{filteredRightIndicesSpan};
  return unfilteredOutput(
      leftTableView,
      filteredLeftIndicesCol,
      rightTableView,
      filteredRightIndicesCol,
      stream);
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::innerJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  auto& hbs = hashObject_.value().second;
  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    // left = probe, right = build
    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      // Make build stream wait for probe tables to become valid
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    auto [leftJoinIndices, rightJoinIndices] = hb->inner_join(
        leftTableView.select(leftKeyIndices_),
        std::nullopt,
        buildStream_.has_value() ? buildStream_.value() : stream);
    if (buildStream_.has_value()) {
      // Make probe stream wait for join completion before using indices
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto leftIndicesSpan =
        cudf::device_span<cudf::size_type const>{*leftJoinIndices};
    auto rightIndicesSpan =
        cudf::device_span<cudf::size_type const>{*rightJoinIndices};
    auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
    auto rightIndicesCol = cudf::column_view{rightIndicesSpan};
    std::vector<std::unique_ptr<cudf::column>> joinedCols;

    if (joinNode_->filter()) {
      cudfOutputs.push_back(filteredOutputIndices(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          cudf::join_kind::INNER_JOIN,
          stream));
    } else {
      cudfOutputs.push_back(unfilteredOutput(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          stream));
    }
  }
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::leftJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  auto& hbs = hashObject_.value().second;
  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    auto [leftJoinIndices, rightJoinIndices] = hb->left_join(
        leftTableView.select(leftKeyIndices_),
        std::nullopt,
        buildStream_.has_value() ? buildStream_.value() : stream);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto leftIndicesSpan =
        cudf::device_span<cudf::size_type const>{*leftJoinIndices};
    auto rightIndicesSpan =
        cudf::device_span<cudf::size_type const>{*rightJoinIndices};
    auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
    auto rightIndicesCol = cudf::column_view{rightIndicesSpan};
    std::vector<std::unique_ptr<cudf::column>> joinedCols;

    if (joinNode_->filter()) {
      cudfOutputs.push_back(filteredOutputIndices(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          cudf::join_kind::LEFT_JOIN,
          stream));
    } else {
      cudfOutputs.push_back(unfilteredOutput(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          stream));
    }
  }
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::rightJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  auto& hbs = hashObject_.value().second;

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    auto [leftJoinIndices, rightJoinIndices] = hb->inner_join(
        leftTableView.select(leftKeyIndices_),
        std::nullopt,
        buildStream_.has_value() ? buildStream_.value() : stream);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }
    // cudf::scatter is async: it enqueues a device memcpy of the old flags
    // (the target) plus a thrust::scatter kernel onto `stream`, then returns
    // immediately. The old rightMatchedFlags_[i] column must stay alive until
    // that work completes. We defer the assignment to rightMatchedFlags_[i]
    // until after unfilteredOutput/filteredOutput (which call
    // stream.synchronize()), so the old column is not destroyed while the
    // scatter kernel is still reading from it.
    std::unique_ptr<cudf::column> updated_flag_col;
    if (!joinNode_->filter()) {
      // Mark matched rights using scatter of true into flags at matching
      // indices
      // TODO (dm): Use a better implementation that doesn't require making a
      // scalar and broadcasting. My cuDF foo is rusty
      auto true_scalar = cudf::numeric_scalar<bool>(true, true, stream);
      auto true_col = cudf::make_column_from_scalar(
          true_scalar,
          rightJoinIndices->size(),
          stream,
          cudf::get_current_device_resource_ref());
      auto flags_table = cudf::table_view({rightMatchedFlags_[i]->view()});
      auto rightIdxCol = cudf::column_view{
          cudf::device_span<cudf::size_type const>{*rightJoinIndices}};
      auto updated_flags_tbl = cudf::scatter(
          cudf::table_view({true_col->view()}),
          rightIdxCol,
          flags_table,
          stream,
          cudf::get_current_device_resource_ref());
      updated_flag_col = std::move(updated_flags_tbl->release()[0]);    
    }

    auto leftIndicesSpan =
        cudf::device_span<cudf::size_type const>{*leftJoinIndices};
    auto rightIndicesSpan =
        cudf::device_span<cudf::size_type const>{*rightJoinIndices};
    auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
    auto rightIndicesCol = cudf::column_view{rightIndicesSpan};
    std::vector<std::unique_ptr<cudf::column>> joinedCols;

    if (joinNode_->filter()) {
      auto& rightMatchedFlags = rightMatchedFlags_[i];
      auto filterFunc =
          [&rightMatchedFlags, &updated_flag_col, rightIndicesSpan, stream](
              std::vector<std::unique_ptr<cudf::column>>&& joinedCols,
              cudf::column_view filterColumn) {
            // apply the filter
            auto filterTable =
                std::make_unique<cudf::table>(std::move(joinedCols));
            auto filteredTable =
                cudf::apply_boolean_mask(*filterTable, filterColumn, stream);
            joinedCols = filteredTable->release();

            // For streaming right join, after applying filter, we record
            // matched right indices filter rightJoinIndices with the same mask
            // to update matched flags
            auto rightIdxCol = cudf::column_view{rightIndicesSpan};
            auto filteredIdxTable = cudf::apply_boolean_mask(
                cudf::table_view{std::vector<cudf::column_view>{rightIdxCol}},
                filterColumn,
                stream);
            auto filteredCols = filteredIdxTable->release();
            auto filteredRightIdxCol = std::move(filteredCols[0]);

            // TODO (dm): The below code is repeated from non-filter case. Find
            // a way to consolidate in future
            auto true_scalar = cudf::numeric_scalar<bool>(true, true, stream);
            auto true_col = cudf::make_column_from_scalar(
                true_scalar,
                filteredRightIdxCol->size(),
                stream,
                cudf::get_current_device_resource_ref());
            auto flags_table = cudf::table_view({rightMatchedFlags->view()});
            auto updated_flags_tbl = cudf::scatter(
                cudf::table_view({true_col->view()}),
                filteredRightIdxCol->view(),
                flags_table,
                stream,
                cudf::get_current_device_resource_ref());
            updated_flag_col = std::move(updated_flags_tbl->release()[0]);
            return std::move(joinedCols);
          };
      cudfOutputs.push_back(filteredOutput(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          filterFunc,
          stream));
    } else {
      cudfOutputs.push_back(unfilteredOutput(
          leftTableView,
          leftIndicesCol,
          rightTableView,
          rightIndicesCol,
          stream));
    }
    // Safe to assign now: stream has been synchronized by the output call above.
    rightMatchedFlags_[i] = std::move(updated_flag_col);
  }
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::leftSemiFilterJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    std::unique_ptr<rmm::device_uvector<cudf::size_type>> leftJoinIndices;

    if (joinNode_->filter()) {
      leftJoinIndices = cudf::mixed_left_semi_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftTableView,
          rightTableView,
          tree_.back(),
          cudf::null_equality::UNEQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    } else {
      cudf::filtered_join filter_join(
          rightTableView.select(rightKeyIndices_),
          cudf::null_equality::UNEQUAL,
          cudf::set_as_build_table::RIGHT,
          stream);
      leftJoinIndices = filter_join.semi_join(
          leftTableView.select(leftKeyIndices_),
          stream,
          cudf::get_current_device_resource_ref());
    }

    auto leftIndicesSpan =
        cudf::device_span<cudf::size_type const>{*leftJoinIndices};
    auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
    auto rightIndicesCol = cudf::empty_like(leftIndicesCol);

    cudfOutputs.push_back(unfilteredOutput(
        leftTableView,
        leftIndicesCol,
        rightTableView,
        rightIndicesCol->view(),
        stream));
  }
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>>
CudfHashJoinProbe::rightSemiFilterJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  auto rightTableView = rightTables[0]->view();

  VELOX_CHECK_EQ(
      rightTables.size(),
      1,
      "Multiple right tables not yet supported for rightSemiFilterJoin");

  std::unique_ptr<rmm::device_uvector<cudf::size_type>> rightJoinIndices;
  if (joinNode_->filter()) {
    rightJoinIndices = cudf::mixed_left_semi_join(
        rightTableView.select(rightKeyIndices_),
        leftTableView.select(leftKeyIndices_),
        rightTableView,
        leftTableView,
        tree_.back(),
        cudf::null_equality::UNEQUAL,
        stream,
        cudf::get_current_device_resource_ref());
  } else {
    cudf::filtered_join filter_join(
        leftTableView.select(leftKeyIndices_),
        cudf::null_equality::UNEQUAL,
        cudf::set_as_build_table::RIGHT,
        stream);
    rightJoinIndices = filter_join.semi_join(
        rightTableView.select(rightKeyIndices_),
        stream,
        cudf::get_current_device_resource_ref());
  }

  auto rightIndicesSpan =
      cudf::device_span<cudf::size_type const>{*rightJoinIndices};
  auto rightIndicesCol = cudf::column_view{rightIndicesSpan};
  auto leftIndicesCol = cudf::empty_like(rightIndicesCol);
  cudfOutputs.push_back(unfilteredOutput(
      leftTableView,
      leftIndicesCol->view(),
      rightTableView,
      rightIndicesCol,
      stream));

  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::antiJoin(
    cudf::table_view leftTableViewParam,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;
  auto& rightTables = hashObject_.value().first;

  VELOX_CHECK_EQ(
      rightTables.size(),
      1,
      "Multiple right tables not yet supported for antiJoin");

  auto rightTableView = rightTables[0]->view();

  // For the special case where we need to drop nulls, we create a local table.
  // Otherwise, we use the input view directly.
  std::unique_ptr<cudf::table> modifiedLeftTable;
  cudf::table_view leftTableView = leftTableViewParam;

  // Special case for null-aware anti join where
  // build table is not empty, no nulls, and probe table has nulls
  if (joinNode_->isNullAware() and !joinNode_->filter()) {
    auto const leftTableHasNulls =
        cudf::has_nulls(leftTableViewParam.select(leftKeyIndices_));
    auto const rightTableHasNulls =
        cudf::has_nulls(rightTableView.select(rightKeyIndices_));
    if (rightTables[0]->num_rows() > 0 and !rightTableHasNulls and
        leftTableHasNulls) {
      // drop nulls on probe table - creates a new table
      modifiedLeftTable =
          cudf::drop_nulls(leftTableViewParam, leftKeyIndices_, stream);
      leftTableView = modifiedLeftTable->view();
    }
  }

  std::unique_ptr<rmm::device_uvector<cudf::size_type>> leftJoinIndices;
  if (joinNode_->filter()) {
    leftJoinIndices = cudf::mixed_left_anti_join(
        leftTableView.select(leftKeyIndices_),
        rightTableView.select(rightKeyIndices_),
        leftTableView,
        rightTableView,
        tree_.back(),
        cudf::null_equality::UNEQUAL,
        stream,
        cudf::get_current_device_resource_ref());
  } else {
    auto const rightTableHasNulls =
        cudf::has_nulls(rightTableView.select(rightKeyIndices_));
    if (joinNode_->isNullAware() and rightTableHasNulls) {
      // empty result
      leftJoinIndices = std::make_unique<rmm::device_uvector<cudf::size_type>>(
          0, stream, cudf::get_current_device_resource_ref());
    } else {
      cudf::filtered_join filter_join(
          rightTableView.select(rightKeyIndices_),
          cudf::null_equality::UNEQUAL,
          cudf::set_as_build_table::RIGHT,
          stream);
      leftJoinIndices = filter_join.anti_join(
          leftTableView.select(leftKeyIndices_),
          stream,
          cudf::get_current_device_resource_ref());
    }
  }

  auto leftIndicesSpan =
      cudf::device_span<cudf::size_type const>{*leftJoinIndices};
  auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
  auto rightIndicesCol = cudf::empty_like(leftIndicesCol);
  cudfOutputs.push_back(unfilteredOutput(
      leftTableView,
      leftIndicesCol,
      rightTableView,
      rightIndicesCol->view(),
      stream));

  return cudfOutputs;
}

RowVectorPtr CudfHashJoinProbe::getOutput() {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinProbe::getOutput";
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  if (finished_) {
    return nullptr;
  }
  // Need either a built hash table or pending build batches.
  if (!hashObject_.has_value() && !buildBatches_.has_value()) {
    return nullptr;
  }

  // Materialize accumulated probe inputs into input_ when the byte/row
  // threshold is reached or no more input is expected. This coalesces many
  // small GPU batches into one large batch, dramatically reducing
  // kernel-launch and stream-synchronize overhead.
  if (!joinNode_->isRightSemiFilterJoin() &&
      !accumulatedProbeInputs_.empty() && input_ == nullptr) {
    auto targetBytes = CudfConfig::getInstance().gpuTargetBatchBytes;
    auto minRows = CudfConfig::getInstance().gpuTargetBatchRows;
    bool thresholdReached;
    if (targetBytes > 0 && minRows > 0) {
      thresholdReached =
          accumulatedProbeBytes_ >= targetBytes ||
          accumulatedProbeRows_ >= minRows;
    } else if (targetBytes > 0) {
      thresholdReached = accumulatedProbeBytes_ >= targetBytes;
    } else if (minRows > 0) {
      thresholdReached = accumulatedProbeRows_ >= minRows;
    } else {
      thresholdReached = !accumulatedProbeInputs_.empty();
    }
    if (thresholdReached || noMoreInput_) {
      if (accumulatedProbeInputs_.size() == 1) {
        input_ = std::move(accumulatedProbeInputs_[0]);
      } else {
        auto stream = accumulatedProbeInputs_[0]->stream();
        auto probeType = joinNode_->sources()[0]->outputType();
        auto tbl =
            getConcatenatedTable(accumulatedProbeInputs_, probeType, stream);
        input_ = std::make_shared<CudfVector>(
            operatorCtx_->pool(),
            probeType,
            tbl->num_rows(),
            std::move(tbl),
            stream);
      }
      accumulatedProbeInputs_.clear();
      accumulatedProbeRows_ = 0;
      accumulatedProbeBytes_ = 0;
      {
        auto lockedStats = stats_.wlock();
        lockedStats->addRuntimeStat(
            "numCoalescedBatches", RuntimeCounter(1));
      }
    }
  }

  if (!input_) {
    // If no more input, emit unmatched-right rows if needed.
    if (joinNode_->isRightJoin() && noMoreInput_ && !finished_ &&
        isLastDriver_ && hashObject_.has_value()) {
      GpuGuard gpuGuard;
      auto& rightTables = hashObject_.value().first;
      auto stream = cudfGlobalStreamPool().get_stream();
      std::vector<std::unique_ptr<cudf::table>> toConcat;
      for (size_t i = 0; i < rightTables.size(); ++i) {
        auto& rightTable = rightTables[i];
        auto n = rightTable->num_rows();
        if (n == 0) {
          continue;
        }
        auto& flags = rightMatchedFlags_[i];
        auto boolMask = cudf::unary_operation(
            flags->view(), cudf::unary_operator::NOT, stream);

        auto rightInput =
            rightTable->view().select(rightColumnIndicesToGather_);
        auto unmatchedRight =
            cudf::apply_boolean_mask(rightInput, boolMask->view(), stream);
        auto m = unmatchedRight->num_rows();
        if (m == 0) {
          continue;
        }
        std::vector<std::unique_ptr<cudf::column>> outCols(outputType_->size());
        auto probeType = joinNode_->sources()[0]->outputType();
        for (int li = 0; li < leftColumnOutputIndices_.size(); ++li) {
          auto outIdx = leftColumnOutputIndices_[li];
          auto probeChannel = leftColumnIndicesToGather_[li];
          auto leftCudfType =
              veloxToCudfTypeId(probeType->childAt(probeChannel));
          auto nullScalar = cudf::make_default_constructed_scalar(
              cudf::data_type{leftCudfType});
          outCols[outIdx] = cudf::make_column_from_scalar(
              *nullScalar, m, stream, cudf::get_current_device_resource_ref());
        }
        auto rightCols = unmatchedRight->release();
        for (int ri = 0; ri < rightColumnOutputIndices_.size(); ++ri) {
          auto outIdx = rightColumnOutputIndices_[ri];
          outCols[outIdx] = std::move(rightCols[ri]);
        }
        toConcat.push_back(std::make_unique<cudf::table>(std::move(outCols)));
      }
      // TODO (dm): We build multiple right chunks only when they are too large
      // to fit in cudf::size_type. In case of a right join which doesn't have a
      // lot of matches we'll get outCols of similar size. This concatenation
      // will overflow. Try emitting result of one right chunk at a time.
      if (!toConcat.empty()) {
        auto out = concatenateTables(std::move(toConcat), stream);
        finished_ = true;
        auto size = out->num_rows();
        if (out->num_columns() == 0 || size == 0) {
          return nullptr;
        }
        return std::make_shared<CudfVector>(
            pool(), outputType_, size, std::move(out), stream);
      }
      finished_ = true;
    }
    return nullptr;
  }

  // Both build and probe data are ready — acquire GPU lock NOW.
  // Lock is per-batch: held only during actual GPU compute, released when
  // getOutput() returns. Never held while waiting for I/O.
  GpuGuard gpuGuard;

  // Build hash table lazily on first batch (under the same lock as probe).
  if (!hashObject_.has_value()) {
    buildHashTable();
  }

  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input_);
  VELOX_CHECK_NOT_NULL(cudfInput);
  auto stream = cudfInput->stream();
  // Use getTableView() to avoid expensive materialization for packed_table.
  // cudfInput is staying alive until the table view is no longer needed.
  auto leftTableView = cudfInput->getTableView();
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Probe table number of columns: " << leftTableView.num_columns();
    VLOG(1) << "Probe table number of rows: " << leftTableView.num_rows();
  }

  auto& rightTables = hashObject_.value().first;
  auto& hbs = hashObject_.value().second;
  for (auto i = 0; i < rightTables.size(); i++) {
    auto& rightTable = rightTables[i];
    auto& hb = hbs[i];
    VELOX_CHECK_NOT_NULL(rightTable);
    if (CudfConfig::getInstance().debugEnabled) {
      if (rightTable != nullptr)
        VLOG(2) << "right_table is not nullptr " << rightTable.get()
                << " hasValue(" << hashObject_.has_value() << ")\n";
      if (hb != nullptr)
        VLOG(2) << "hb is not nullptr " << hb.get() << " hasValue("
                << hashObject_.has_value() << ")\n";
    }
  }

  gpuTimer_.start(stream);

  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;
  switch (joinNode_->joinType()) {
    case core::JoinType::kInner:
      cudfOutputs = innerJoin(leftTableView, stream);
      break;
    case core::JoinType::kLeft:
      cudfOutputs = leftJoin(leftTableView, stream);
      break;
    case core::JoinType::kRight:
      cudfOutputs = rightJoin(leftTableView, stream);
      break;
    case core::JoinType::kLeftSemiFilter:
      cudfOutputs = leftSemiFilterJoin(leftTableView, stream);
      break;
    case core::JoinType::kRightSemiFilter:
      cudfOutputs = rightSemiFilterJoin(leftTableView, stream);
      break;
    case core::JoinType::kAnti:
      cudfOutputs = antiJoin(leftTableView, stream);
      break;
    default:
      VELOX_FAIL("Unsupported join type: ", joinNode_->joinType());
  }

  gpuTimer_.stop(stream);

  // Release input CudfVector to free GPU memory before creating output.
  // This reduces peak memory from (input + output) to max(input, output).
  // cudfInput must be released first since input_.reset() only decrements
  // the refcount while cudfInput still holds a reference.
  cudfInput.reset();
  input_.reset();
  finished_ = noMoreInput_ && !joinNode_->isRightJoin();

  auto cudfOutput = concatenateTables(std::move(cudfOutputs), stream);
  auto const size = cudfOutput->num_rows();
  if (cudfOutput->num_columns() == 0 or size == 0) {
    return nullptr;
  }
  return std::make_shared<CudfVector>(
      pool(),
      outputType_,
      cudfOutput->num_rows(),
      std::move(cudfOutput),
      stream);
}

bool CudfHashJoinProbe::skipProbeOnEmptyBuild() const {
  auto const joinType = joinNode_->joinType();
  return isInnerJoin(joinType) || isLeftSemiFilterJoin(joinType) ||
      isRightJoin(joinType) || isRightSemiFilterJoin(joinType) ||
      isRightSemiProjectJoin(joinType);
}

exec::BlockingReason CudfHashJoinProbe::isBlocked(ContinueFuture* future) {
  if ((joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin()) &&
      hashObject_.has_value()) {
    if (!future_.valid()) {
      return exec::BlockingReason::kNotBlocked;
    }
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForJoinProbe;
  }

  if (hashObject_.has_value() || buildBatches_.has_value()) {
    return exec::BlockingReason::kNotBlocked;
  }

  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  auto cudfJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  VELOX_CHECK_NOT_NULL(cudfJoinBridge);
  VELOX_CHECK_NOT_NULL(future);
  auto batches = cudfJoinBridge->buildBatchesOrFuture(future);

  if (!batches.has_value()) {
    VLOG_IF(2, CudfConfig::getInstance().debugEnabled)
        << "CudfHashJoinProbe is blocked, waiting for join build";
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  buildBatches_ = std::move(batches);

  // Detect empty build from raw batch row counts (no GPU work needed).
  int64_t totalBuildRows = 0;
  for (auto& batch : buildBatches_.value()) {
    totalBuildRows += batch.numRows;
  }
  if (totalBuildRows == 0) {
    if (skipProbeOnEmptyBuild()) {
      if (operatorCtx_->driverCtx()
              ->queryConfig()
              .hashProbeFinishEarlyOnEmptyBuild()) {
        noMoreInput();
      } else {
        skipInput_ = true;
      }
    }
  }

  if ((joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin()) &&
      future_.valid()) {
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForJoinProbe;
  }
  return exec::BlockingReason::kNotBlocked;
}

bool CudfHashJoinProbe::isFinished() {
  auto const isFinished = finished_ ||
      (noMoreInput_ && input_ == nullptr && accumulatedProbeInputs_.empty());

  if (isFinished) {
    hashObject_.reset();
    buildBatches_.reset();
  }
  return isFinished;
}

std::unique_ptr<exec::Operator> CudfHashJoinBridgeTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridgeTranslator::toOperator";
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    return std::make_unique<CudfHashJoinProbe>(id, ctx, joinNode);
  }
  return nullptr;
}

std::unique_ptr<exec::JoinBridge> CudfHashJoinBridgeTranslator::toJoinBridge(
    const core::PlanNodePtr& node) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridgeTranslator::toJoinBridge";
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    auto joinBridge = std::make_unique<CudfHashJoinBridge>();
    return joinBridge;
  }
  return nullptr;
}

exec::OperatorSupplier CudfHashJoinBridgeTranslator::toOperatorSupplier(
    const core::PlanNodePtr& node) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridgeTranslator::toOperatorSupplier";
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    return [joinNode](int32_t operatorId, exec::DriverCtx* ctx) {
      return std::make_unique<CudfHashJoinBuild>(operatorId, ctx, joinNode);
    };
  }
  return nullptr;
}

} // namespace facebook::velox::cudf_velox
