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
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/AstExpression.h"
#include "velox/experimental/cudf/expression/AstExpressionUtils.h"
#include "velox/experimental/cudf/expression/DecimalUtils.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h" // NOLINT(misc-unused-headers)
#include "velox/type/TypeUtil.h"

#include <cudf/aggregation.hpp>
#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/filling.hpp>
#include <cudf/groupby.hpp>
#include <cudf/join/filtered_join.hpp>
#include <cudf/join/join.hpp>
#include <cudf/join/mixed_join.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/search.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/table/table.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/unary.hpp>

#include <cuda_runtime_api.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/error.hpp>
#include <rmm/exec_policy.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>

#include <nvtx3/nvtx3.hpp>

namespace facebook::velox::cudf_velox {

namespace {

static constexpr int kOomMaxRetries = 10;

// Maximum cumulative time (ms) spent on OOM retries per getOutput() call.
// (retained for possible future use in joinWithAdaptiveSplit enhancements)
[[maybe_unused]] static constexpr int64_t kMaxRetryTotalMs = 300000;

// Serialization for large hash joins. When a join's estimated memory
// footprint (build table + hash table + join output) exceeds this
// fraction of total GPU memory, acquire exclusive access to prevent
// cross-task RMM pool corruption from concurrent large allocations.
// With maxConcurrentGpuTasks=3 and 22 GB RMM pool, two large joins
// can simultaneously exhaust the pool; this mutex ensures only one
// heavy join runs at a time while allowing small joins full parallelism.
static std::mutex sLargeJoinMutex;
static constexpr double kLargeJoinMemoryFraction = 0.30;


void recoverGpuMemory() {
  auto syncErr = cudaDeviceSynchronize();
  auto lastErr = cudaGetLastError();
  // OOM and invalid-value errors are expected and recoverable.
  // cudaErrorInvalidValue typically indicates an oversized allocation request
  // (e.g. join output exceeding GPU memory), NOT device corruption.
  // Fatal errors (illegal address, assert, device unavailable) mean the
  // context is corrupted -- fail fast instead of proceeding to corrupt the
  // RMM pool further.
  auto err = (syncErr != cudaSuccess) ? syncErr : lastErr;
  if (err != cudaSuccess && err != cudaErrorMemoryAllocation &&
      err != cudaErrorInvalidValue) {
    VELOX_FAIL(
        "Fatal CUDA error during GPU memory recovery: {} ({}). "
        "Device context is corrupted.",
        cudaGetErrorString(err),
        static_cast<int>(err));
  }
}

// Check for sticky CUDA errors and throw if the device is corrupted.
// Must be called after stream.synchronize() to detect async errors
// before they corrupt the RMM pool during subsequent deallocations.
void checkCudaHealth(const char* context) {
  auto err = cudaGetLastError();
  if (err == cudaSuccess || err == cudaErrorInvalidValue) {
    // cudaErrorInvalidValue is NOT device corruption — it indicates an
    // oversized or invalid allocation parameter. Clearing it here prevents
    // stale errors from cascading to subsequent operations.
    return;
  }
  auto msg = cudaGetErrorString(err);
  VELOX_FAIL(
      "CUDA device error detected at {}: {} ({}). "
      "GPU context may be corrupted -- failing fast to prevent "
      "RMM pool metadata corruption.",
      context,
      msg,
      static_cast<int>(err));
}

size_t freeGpuMemoryBytes() {
  size_t freeMem = 0, totalMem = 0;
  if (cudaMemGetInfo(&freeMem, &totalMem) != cudaSuccess) {
    cudaGetLastError();
    return 0;
  }
  return freeMem;
}

void ensureGpuMemoryAvailable(size_t desiredBytes, const char* context) {
  size_t freeMem = freeGpuMemoryBytes();
  if (freeMem > 0 && freeMem < desiredBytes) {
    LOG(INFO) << context << ": free GPU memory " << (freeMem >> 20)
              << "MB < desired " << (desiredBytes >> 20)
              << "MB, recovering deferred frees";
    recoverGpuMemory();
    checkCudaHealth(context);
  }
}

bool isCudaRelatedError(const std::exception& e) {
  if (dynamic_cast<const std::bad_alloc*>(&e) != nullptr) {
    return true;
  }
  if (dynamic_cast<const rmm::cuda_error*>(&e) != nullptr) {
    return true;
  }
  std::string what = e.what();
  return what.find("cudaError") != std::string::npos ||
      what.find("CUDA error") != std::string::npos ||
      what.find("out_of_memory") != std::string::npos;
}

// Detect fatal (non-OOM) CUDA errors from the exception message.
// rmm::cuda_error clears the sticky error via cudaGetLastError() in its
// constructor, so cudaPeekAtLastError() often returns cudaSuccess even when
// the GPU context is corrupted. Checking the exception message is the only
// reliable way to distinguish OOM (retriable) from fatal errors.
bool isFatalCudaError(const std::exception& e) {
  std::string what = e.what();
  // cudaErrorInvalidValue is intentionally NOT in this list. It typically
  // indicates an oversized allocation request (e.g., when join output exceeds
  // INT32_MAX rows and buffer size wraps). The GPU context is NOT corrupted,
  // so the retry/split mechanism in getOutput() can recover by processing
  // smaller probe chunks.
  static const char* fatalPatterns[] = {
      "cudaErrorIllegalAddress",
      "cudaErrorIllegalInstruction",
      "cudaErrorMisalignedAddress",
      "cudaErrorInvalidConfiguration",
      "cudaErrorInvalidDevice",
      "cudaErrorInvalidPitchValue",
      "cudaErrorDevicesUnavailable",
      "cudaErrorAssert",
      "cudaErrorECCUncorrectable",
      "cudaErrorUnknown",
  };
  for (const auto* pat : fatalPatterns) {
    if (what.find(pat) != std::string::npos) {
      return true;
    }
  }
  return false;
}

// Lightweight null-key row counting using bitmask operations.
// Much cheaper than cudf::drop_nulls which allocates a full table copy.
// Returns the number of rows where ANY key column contains a null.
// Only allocates O(N/8) bytes for the combined bitmask vs. O(N * total_cols *
// bytes_per_col) for drop_nulls. Returns -1 on failure (caller should skip).
int64_t countNullKeyRows(
    cudf::table_view const& table,
    std::vector<cudf::size_type> const& keyIndices,
    rmm::cuda_stream_view stream) {
  if (table.num_rows() == 0 || keyIndices.empty()) {
    return 0;
  }

  // Fast path: check if any key column has nulls at all.
  bool anyNulls = false;
  int nullableKeyCount = 0;
  cudf::size_type singleNullableIdx = -1;
  for (auto idx : keyIndices) {
    if (table.column(idx).has_nulls()) {
      anyNulls = true;
      nullableKeyCount++;
      singleNullableIdx = idx;
    }
  }
  if (!anyNulls) {
    return 0;
  }

  // Single nullable key: use its null_count() directly (free metadata).
  if (nullableKeyCount == 1) {
    return static_cast<int64_t>(table.column(singleNullableIdx).null_count());
  }

  // Multiple nullable keys: use bitmask_and on the key columns to compute
  // the combined validity mask. This allocates only a single bitmask buffer.
  auto keyTable = table.select(keyIndices);
  auto [combinedMask, nullCount] = cudf::bitmask_and(keyTable, stream);
  return static_cast<int64_t>(nullCount);
}

/// Creates extended table view by appending precomputed columns
cudf::table_view createExtendedTableView(
    cudf::table_view originalView,
    std::vector<ColumnOrView>& precomputedColumns) {
  if (precomputedColumns.empty()) {
    return originalView;
  }

  std::vector<cudf::column_view> allViews;
  allViews.reserve(originalView.num_columns() + precomputedColumns.size());

  for (cudf::size_type i = 0; i < originalView.num_columns(); ++i) {
    allViews.push_back(originalView.column(i));
  }
  for (auto& col : precomputedColumns) {
    allViews.push_back(asView(col));
  }

  return cudf::table_view(allViews);
}

// Cast probe-side key columns to match build-side types when decimal
// precision/scale mismatches exist. Returns a new table view with
// aligned key columns; 'castColumns' holds the casted column ownership.
cudf::table_view alignProbeKeyTypes(
    cudf::table_view probeView,
    const std::vector<cudf::size_type>& probeKeyIndices,
    cudf::table_view buildView,
    const std::vector<cudf::size_type>& buildKeyIndices,
    std::vector<std::unique_ptr<cudf::column>>& castColumns,
    rmm::cuda_stream_view stream) {
  bool needCast = false;
  for (size_t k = 0; k < probeKeyIndices.size(); ++k) {
    auto pType = probeView.column(probeKeyIndices[k]).type();
    auto bType = buildView.column(buildKeyIndices[k]).type();
    if (cudf::is_fixed_point(pType) && cudf::is_fixed_point(bType) &&
        pType != bType) {
      needCast = true;
      break;
    }
  }
  if (!needCast) return probeView;

  std::vector<cudf::column_view> cols;
  cols.reserve(probeView.num_columns());
  for (cudf::size_type c = 0; c < probeView.num_columns(); ++c) {
    cols.push_back(probeView.column(c));
  }
  for (size_t k = 0; k < probeKeyIndices.size(); ++k) {
    auto pi = probeKeyIndices[k];
    auto bType = buildView.column(buildKeyIndices[k]).type();
    if (cudf::is_fixed_point(cols[pi].type()) &&
        cudf::is_fixed_point(bType) && cols[pi].type() != bType) {
      auto casted = cudf::cast(cols[pi], bType, stream,
          cudf::get_current_device_resource_ref());
      cols[pi] = casted->view();
      castColumns.push_back(std::move(casted));
    }
  }
  return cudf::table_view(cols);
}

// Compute the maximum number of output rows that can be safely gathered
// in a single cudf::gather() call. Prevents cudaErrorInvalidValue from
// oversized device_buffer allocations and respects cudf::size_type limits.
size_t computeMaxGatherRows(
    cudf::table_view leftTableView,
    cudf::table_view rightTableView) {
  size_t bytesPerRow = 0;
  for (cudf::size_type c = 0; c < leftTableView.num_columns(); ++c) {
    auto dt = leftTableView.column(c).type();
    bytesPerRow += cudf::is_fixed_width(dt) ? cudf::size_of(dt) : 32;
  }
  for (cudf::size_type c = 0; c < rightTableView.num_columns(); ++c) {
    auto dt = rightTableView.column(c).type();
    bytesPerRow += cudf::is_fixed_width(dt) ? cudf::size_of(dt) : 32;
  }
  bytesPerRow = std::max(bytesPerRow, size_t(8));

  auto maxRows =
      static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());

  size_t freeMem = freeGpuMemoryBytes();
  if (freeMem > 0) {
    // Use 5% of free memory per gather batch (down from 20%).
    // With concurrent GPU tasks, 20% frequently exhausted available
    // memory during the gather causing cudaErrorInvalidValue.
    size_t memBased = (freeMem / 20) / bytesPerRow;
    maxRows = std::min(maxRows, std::max(memBased, size_t(1024)));
  }
  return maxRows;
}

// Recursive probe-splitting wrapper for join calls. On CUDA error or
// size_type overflow, splits the probe table in half and retries each half
// independently. This catches and handles errors INSIDE the operator before
// they propagate to Velox's error framework (where they become non-retriable).
// Accumulates results from all successful sub-joins.
//
// When the probe can't be split further (1 row or max depth) and retries
// fail, invokes buildChunkFallbackFn if provided. This splits the BUILD
// table into smaller chunks and joins each independently -- essential when
// a single probe row matches millions of build rows.
template <typename JoinFunc, typename FallbackFunc>
std::vector<std::unique_ptr<cudf::table>> joinWithAdaptiveSplit(
    cudf::table_view probeView,
    rmm::cuda_stream_view stream,
    JoinFunc&& joinFn,
    const std::string& context,
    FallbackFunc&& buildChunkFallbackFn,
    int depth = 0) {
  static constexpr int kMaxSplitDepth = 30;
  static constexpr int kMaxLeafRetries = 3;

  try {
    return joinFn(probeView);
  } catch (const std::exception& e) {
    bool isOverflow = std::string(e.what()).find(
        "exceeds cudf::size_type limit") != std::string::npos;

    if (!isOverflow && !isCudaRelatedError(e)) {
      throw;
    }
    if (!isOverflow && isFatalCudaError(e)) {
      throw;
    }

    // Check for unrecoverable CUDA device corruption.
    if (!isOverflow) {
      auto err = cudaPeekAtLastError();
      if (err != cudaSuccess && err != cudaErrorMemoryAllocation &&
          err != cudaErrorInvalidValue) {
        throw;
      }
    }

    cudaGetLastError();
    recoverGpuMemory();

    // If we can't split further, retry with exponential backoff
    // (other concurrent tasks may free GPU memory during the wait).
    if (probeView.num_rows() <= 1 || depth >= kMaxSplitDepth) {
      for (int attempt = 0; attempt < kMaxLeafRetries; ++attempt) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(200 * (1 << attempt)));
        recoverGpuMemory();
        try {
          return joinFn(probeView);
        } catch (const std::exception& retryErr) {
          bool retryOverflow = std::string(retryErr.what()).find(
              "exceeds cudf::size_type limit") != std::string::npos;
          if (!retryOverflow && !isCudaRelatedError(retryErr)) {
            throw;
          }
          if (!retryOverflow && isFatalCudaError(retryErr)) {
            throw;
          }
          cudaGetLastError();
          recoverGpuMemory();
        }
      }

      // All retries exhausted -- try build-side chunking as last resort.
      // A single probe row can match millions of build rows (e.g. TPC-DS
      // Q4/Q11 on low-cardinality keys), producing output that exceeds
      // GPU memory in the gather map allocation. Splitting the BUILD table
      // into smaller chunks bounds the per-chunk output size.
      try {
        auto fallbackResult = buildChunkFallbackFn(probeView, stream);
        if (!fallbackResult.empty()) {
          LOG(INFO) << context
                    << ": build-side chunking succeeded for "
                    << probeView.num_rows() << " probe rows"
                    << " (depth=" << depth << ")";
          return fallbackResult;
        }
      } catch (const std::exception& fbErr) {
        LOG(WARNING) << context
                     << ": build-side chunking also failed: "
                     << fbErr.what();
        recoverGpuMemory();
      }

      throw;
    }

    auto half = static_cast<cudf::size_type>(probeView.num_rows() / 2);
    auto splits = cudf::split(probeView, {half}, stream);

    LOG(WARNING) << context << ": "
                 << (isOverflow ? "overflow" : "CUDA error")
                 << " with " << probeView.num_rows() << " probe rows"
                 << ", splitting -> " << splits[0].num_rows()
                 << " + " << splits[1].num_rows()
                 << " (depth=" << depth << "): " << e.what();

    auto results = joinWithAdaptiveSplit(
        splits[0], stream, std::forward<JoinFunc>(joinFn),
        context, std::forward<FallbackFunc>(buildChunkFallbackFn),
        depth + 1);
    auto results2 = joinWithAdaptiveSplit(
        splits[1], stream, std::forward<JoinFunc>(joinFn),
        context, std::forward<FallbackFunc>(buildChunkFallbackFn),
        depth + 1);
    results.insert(results.end(),
        std::make_move_iterator(results2.begin()),
        std::make_move_iterator(results2.end()));
    return results;
  }
}

// Maximum number of join output rows whose gather map indices (two int32
// device_uvectors) can be safely allocated. Limits based on free GPU
// memory and cudf::size_type range to prevent cudaErrorInvalidValue from
// oversized allocations.
size_t computeMaxSafeJoinOutputRows(rmm::cuda_stream_view stream) {
  auto maxInt =
      static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());
  size_t freeMem = freeGpuMemoryBytes();
  if (freeMem > 0) {
    // Each output row costs 8 bytes for the gather map pair (2 × int32).
    // Reserve at most 5% of free memory for gather maps. Conservative
    // because output table materialization also needs substantial memory
    // and concurrent GPU tasks compete for the RMM pool.
    size_t memLimit = (freeMem / 20) / 8;
    return std::min(maxInt, std::max(memLimit, size_t(1024)));
  }
  return maxInt;
}

// Throw a recognizable overflow exception if the estimated join output
// exceeds safe GPU memory limits. The message contains "exceeds
// cudf::size_type limit" to trigger the probe-splitting path in
// joinWithAdaptiveSplit WITHOUT going through CUDA error handling.
void throwIfJoinOutputTooLarge(
    size_t estimatedRows,
    cudf::table_view probeView,
    cudf::table_view buildView,
    const std::string& context) {
  size_t bytesPerRow = 8; // gather map pair (2 x int32)
  for (cudf::size_type c = 0; c < probeView.num_columns(); ++c) {
    auto dt = probeView.column(c).type();
    bytesPerRow += cudf::is_fixed_width(dt) ? cudf::size_of(dt) : 32;
  }
  for (cudf::size_type c = 0; c < buildView.num_columns(); ++c) {
    auto dt = buildView.column(c).type();
    bytesPerRow += cudf::is_fixed_width(dt) ? cudf::size_of(dt) : 32;
  }
  bytesPerRow = std::max(bytesPerRow, size_t(16));

  auto maxInt =
      static_cast<size_t>(std::numeric_limits<cudf::size_type>::max());
  size_t maxRows = maxInt;
  size_t freeMem = freeGpuMemoryBytes();
  if (freeMem > 0) {
    size_t memLimit = (freeMem / 10) / bytesPerRow;
    maxRows = std::min(maxInt, std::max(memLimit, size_t(1024)));
  }

  if (estimatedRows > maxRows) {
    VELOX_FAIL(
        "{}: estimated join output {} rows exceeds safe limit {} rows "
        "(exceeds cudf::size_type limit). "
        "bytesPerRow={}, freeMem={}MB",
        context,
        estimatedRows,
        maxRows,
        bytesPerRow,
        freeMem >> 20);
  }
}

} // namespace

void CudfHashJoinProbe::close() {
  Operator::close();
  filterEvaluator_.reset();
  scalars_.clear();
  tree_ = {};
  accumulatedProbeInputs_.clear();
  accumulatedProbeRows_ = 0;
  accumulatedProbeBytes_ = 0;
}

void CudfHashJoinBridge::setHashTable(
    std::optional<CudfHashJoinBridge::hash_type> hashObject) {
  if (CudfConfig::getInstance().debugEnabled) {
    if (hashObject.has_value()) {
      const auto& [tables, joins] = hashObject.value();
      VLOG(1) << "Calling CudfHashJoinBridge::setHashTable with tables="
              << tables.size() << ", hashObjects=" << joins.size();
      for (size_t i = 0; i < tables.size(); ++i) {
        VLOG(2) << "  setHashTable table[" << i
                << "]=" << static_cast<const void*>(tables[i].get());
      }
      for (size_t i = 0; i < joins.size(); ++i) {
        VLOG(2) << "  setHashTable hashObject[" << i
                << "]=" << static_cast<const void*>(joins[i].get());
      }
    } else {
      VLOG(1) << "Calling CudfHashJoinBridge::setHashTable with nullopt";
    }
  }
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(
        !hashObject_.has_value(),
        "CudfHashJoinBridge already has a hash table");
    hashObject_ = std::move(hashObject);
    promises = std::move(promises_);
    if (CudfConfig::getInstance().debugEnabled) {
      VLOG(1) << "CudfHashJoinBridge::setHashTable stored hash table; waiters="
              << promises.size();
    }
  }
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBridge::setHashTable notifying waiters";
  }
  notify(std::move(promises));
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBridge::setHashTable completed";
  }
}

std::optional<CudfHashJoinBridge::hash_type> CudfHashJoinBridge::hashOrFuture(
    ContinueFuture* future) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridge::hashOrFuture";
  }
  std::lock_guard<std::mutex> l(mutex_);
  if (hashObject_.has_value()) {
    return hashObject_;
  }
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridge::hashOrFuture constructing promise";
  }
  promises_.emplace_back("CudfHashJoinBridge::hashOrFuture");
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridge::hashOrFuture getSemiFuture";
  }
  *future = promises_.back().getSemiFuture();
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinBridge::hashOrFuture returning nullopt";
  }
  return std::nullopt;
}

void CudfHashJoinBridge::setBuildStream(rmm::cuda_stream_view buildStream) {
  std::lock_guard<std::mutex> l(mutex_);
  buildStream_ = buildStream;
}

std::optional<rmm::cuda_stream_view> CudfHashJoinBridge::getBuildStream() {
  std::lock_guard<std::mutex> l(mutex_);
  return buildStream_;
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
  // Queue inputs, process all at once.
  if (input->size() > 0) {
    auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
    VELOX_CHECK_NOT_NULL(cudfInput);
    inputs_.push_back(std::move(cudfInput));
  }
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
  GpuGuard gpuGuard;
  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // Only last driver collects all answers
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }
  // Collect results from peers
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* build = dynamic_cast<CudfHashJoinBuild*>(op);
    VELOX_CHECK_NOT_NULL(build);
    inputs_.insert(inputs_.end(), build->inputs_.begin(), build->inputs_.end());
  }

  SCOPE_EXIT {
    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  };

  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBuild: build batches";
    VLOG(1) << "Build batches count: " << inputs_.size();
    if (inputs_.empty()) {
      VLOG(1) << "Build input batches are empty; continuing with empty build table";
    } else {
      VELOX_CHECK_NOT_NULL(inputs_[0]);
      VLOG(1) << "Build batches number of columns: "
              << inputs_[0]->getTableView().num_columns();
    }
    for (size_t i = 0; i < inputs_.size(); i++) {
      VELOX_CHECK_NOT_NULL(inputs_[i]);
      VLOG(1) << "Build batch " << i
              << ": number of rows: " << inputs_[i]->getTableView().num_rows();
    }
  }

  auto stream = cudfGlobalStreamPool().get_stream();

  // Reclaim deferred async frees before attempting a large allocation.
  // With many concurrent tasks, cudaFreeAsync defers actual deallocation;
  // this forces those frees, reducing the chance of OOM during concatenation.
  ensureGpuMemoryAvailable(256ULL << 20, "CudfHashJoinBuild::noMoreInput");

  std::vector<std::unique_ptr<cudf::table>> tbls;
  for (int attempt = 0;; ++attempt) {
    try {
      tbls = getConcatenatedTableBatched(
          inputs_, joinNode_->sources()[1]->outputType(), stream);
      break;
    } catch (const std::exception& e) {
      if (!isCudaRelatedError(e)) {
        throw;
      }
      // If the device has a sticky error (not just OOM or invalid-value),
      // retrying is futile and will only cause timeouts or RMM corruption.
      // cudaErrorInvalidValue is NOT device corruption — it means an
      // allocation request had invalid parameters (typically oversized).
      {
        auto err = cudaPeekAtLastError();
        if (err != cudaSuccess && err != cudaErrorMemoryAllocation &&
            err != cudaErrorInvalidValue) {
          VELOX_FAIL(
              "CUDA device error {} ({}) during build concatenation for "
              "planNode {}. Aborting: {}",
              static_cast<int>(err),
              cudaGetErrorString(err),
              planNodeId(),
              e.what());
        }
        if (err == cudaErrorInvalidValue) {
          cudaGetLastError(); // clear the non-sticky error
        }
      }
      if (isFatalCudaError(e)) {
        VELOX_FAIL(
            "Fatal CUDA error during build concatenation for planNode {} "
            "(detected from exception message). Aborting: {}",
            planNodeId(),
            e.what());
      }
      // For cudaErrorInvalidValue the batch is too large (not transient OOM);
      // retrying just corrupts CUDA state further. Fall back immediately.
      // Cap OOM retries at 3 for build concatenation (vs 10 for probe) —
      // build concat failures are typically size-based, not transient.
      bool isInvalidValue = std::string(e.what()).find(
          "cudaErrorInvalidValue") != std::string::npos;
      static constexpr int kBuildConcatMaxRetries = 3;
      if (isInvalidValue || attempt >= kBuildConcatMaxRetries) {
        LOG(WARNING)
            << "CudfHashJoinBuild: concatenation failed"
            << (isInvalidValue ? " (cudaErrorInvalidValue, immediate fallback)"
                               : " after retries")
            << " for planNode " << planNodeId()
            << ". Falling back to per-batch build tables ("
            << inputs_.size() << " batches): " << e.what();
        // Full CUDA state recovery: synchronize device, then clear any
        // sticky error BEFORE touching per-batch streams. Without this,
        // a pending error from the failed concatenation can cause SIGSEGV
        // in stream.synchronize() during the per-batch release below.
        cudaDeviceSynchronize();
        cudaGetLastError();
        // First try release() (zero-copy move). If that fails for a
        // batch (e.g. stream sync error), fall back to copying from the
        // table view which avoids per-batch stream synchronization.
        for (auto& inp : inputs_) {
          if (!inp || inp->size() == 0) {
            continue;
          }
          try {
            tbls.push_back(inp->release());
          } catch (const std::exception& releaseErr) {
            LOG(WARNING)
                << "CudfHashJoinBuild: release() failed for planNode "
                << planNodeId() << ": " << releaseErr.what()
                << ". Trying table copy from view.";
            cudaGetLastError();
            try {
              tbls.push_back(std::make_unique<cudf::table>(
                  inp->getTableView(), stream,
                  cudf::get_current_device_resource_ref()));
            } catch (const std::exception& copyErr) {
              LOG(WARNING)
                  << "CudfHashJoinBuild: table copy also failed for "
                  << "planNode " << planNodeId() << ": " << copyErr.what();
              cudaGetLastError();
            }
          }
        }
        if (tbls.empty()) {
          tbls.push_back(makeEmptyTable(
              joinNode_->sources()[1]->outputType()));
        }
        break;
      }
      LOG(WARNING)
          << "CudfHashJoinBuild OOM during concatenation for planNode "
          << planNodeId() << " (attempt " << (attempt + 1) << "/"
          << kBuildConcatMaxRetries << "): " << e.what()
          << ". Recovering GPU memory and retrying.";
      recoverGpuMemory();
      std::this_thread::sleep_for(
          std::chrono::milliseconds(200 * (1 << attempt)));
    }
  }
  inputs_.clear();

  for (auto const& tbl : tbls) {
    VELOX_CHECK_NOT_NULL(tbl);
  }
  VELOX_CHECK(
      !tbls.empty(),
      "Expected at least one build table after concatenation. planNodeId: {}",
      planNodeId());
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Build table batches count: " << tbls.size();
    VLOG(1) << "Build table number of columns: " << tbls[0]->num_columns();
    for (size_t i = 0; i < tbls.size(); i++) {
      VLOG(1) << "Build table " << i
              << ": number of rows: " << tbls[i]->num_rows();
    }
  }

  auto rightKeys = joinNode_->rightKeys();

  auto buildKeyIndices = std::vector<cudf::size_type>(rightKeys.size());
  auto buildType = joinNode_->sources()[1]->outputType();
  for (size_t i = 0; i < buildKeyIndices.size(); i++) {
    buildKeyIndices[i] = static_cast<cudf::size_type>(
        buildType->getChildIdx(rightKeys[i]->name()));
  }

  {
    int64_t totalNullKeyRows = 0;
    for (const auto& tbl : tbls) {
      if (tbl->num_rows() == 0) {
        continue;
      }
      try {
        auto nullRows = countNullKeyRows(tbl->view(), buildKeyIndices, stream);
        if (nullRows > 0) {
          totalNullKeyRows += nullRows;
        }
      } catch (const std::exception& e) {
        if (!isCudaRelatedError(e)) {
          throw;
        }
        LOG(WARNING)
            << "CudfHashJoinBuild: null key counting failed for planNode "
            << planNodeId() << " (" << tbl->num_rows() << " rows): "
            << e.what() << ". Skipping null key stats.";
        cudaGetLastError();
      }
    }
    auto lockedStats = stats_.wlock();
    lockedStats->numNullKeys += totalNullKeyRows;
  }

  // Hash table construction is deferred to the probe side (getOutput).
  // With N concurrent tasks, building hash tables here causes all N sets
  // to persist in GPU memory simultaneously (GpuGuard only limits
  // concurrent execution, not memory residency). Deferring to probe
  // ensures at most GpuGuard-max hash table sets exist at any time.
  std::vector<std::shared_ptr<cudf::hash_join>> hashObjects(
      tbls.size(), nullptr);

  std::vector<std::shared_ptr<cudf::table>> shared_tbls;
  for (auto& tbl : tbls) {
    shared_tbls.push_back(std::move(tbl));
  }
  VELOX_CHECK_EQ(
      shared_tbls.size(),
      hashObjects.size(),
      "Mismatched build table/hash object counts. planNodeId: {}, splitGroupId: {}",
      planNodeId(),
      operatorCtx_->driverCtx()->splitGroupId);
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Prepared hash join payload: shared_tbls=" << shared_tbls.size()
            << ", hashObjects=" << hashObjects.size();
    for (size_t i = 0; i < shared_tbls.size(); ++i) {
      VLOG(2) << "  shared_tbls[" << i
              << "]=" << static_cast<const void*>(shared_tbls[i].get());
    }
    for (size_t i = 0; i < hashObjects.size(); ++i) {
      VLOG(2) << "  hashObjects[" << i
              << "]=" << static_cast<const void*>(hashObjects[i].get());
    }
  }
  // set hash table to CudfHashJoinBridge
  const auto splitGroupId = operatorCtx_->driverCtx()->splitGroupId;
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      splitGroupId, planNodeId());
  auto cudfHashJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBuild bridge lookup: planNodeId=" << planNodeId()
            << ", splitGroupId=" << splitGroupId
            << ", joinBridge=" << static_cast<const void*>(joinBridge.get())
            << ", cudfHashJoinBridge="
            << static_cast<const void*>(cudfHashJoinBridge.get());
  }
  VELOX_CHECK_NOT_NULL(
      joinBridge,
      "Expected JoinBridge for CudfHashJoinBuild. planNodeId: {}, splitGroupId: {}",
      planNodeId(),
      splitGroupId);
  VELOX_CHECK_NOT_NULL(
      cudfHashJoinBridge,
      "Expected CudfHashJoinBridge for CudfHashJoinBuild. planNodeId: {}, splitGroupId: {}, joinBridge: {}",
      planNodeId(),
      splitGroupId,
      static_cast<const void*>(joinBridge.get()));

  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBuild setting build stream: planNodeId="
            << planNodeId() << ", splitGroupId=" << splitGroupId;
  }
  // Synchronize and check for async CUDA errors from build-side
  // concatenation before handing tables to the probe. Without this,
  // corrupted table data from a build-side kernel error would silently
  // propagate and corrupt the RMM pool when the probe dereferences it.
  stream.synchronize();
  checkCudaHealth("CudfHashJoinBuild::noMoreInput before bridge handoff");

  cudfHashJoinBridge->setBuildStream(stream);
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBuild setBuildStream completed: planNodeId="
            << planNodeId() << ", splitGroupId=" << splitGroupId;
    VLOG(1) << "CudfHashJoinBuild setting hash table: planNodeId="
            << planNodeId() << ", splitGroupId=" << splitGroupId;
  }
  cudfHashJoinBridge->setHashTable(
      std::make_optional(
          std::make_pair(std::move(shared_tbls), std::move(hashObjects))));
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinBuild setHashTable completed: planNodeId="
            << planNodeId() << ", splitGroupId=" << splitGroupId;
  }
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
      probeType_(joinNode_->sources()[0]->outputType()),
      buildType_(joinNode_->sources()[1]->outputType()),
      cudaEvent_(std::make_unique<CudaEvent>(cudaEventDisableTiming)) {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "CudfHashJoinProbe constructor";
  }
  auto const& leftKeys = joinNode_->leftKeys(); // probe keys
  auto const& rightKeys = joinNode_->rightKeys(); // build keys

  if (CudfConfig::getInstance().debugEnabled) {
    for (int i = 0; i < probeType_->names().size(); i++) {
      VLOG(1) << "Left column " << i << ": " << probeType_->names()[i];
    }

    for (int i = 0; i < buildType_->names().size(); i++) {
      VLOG(1) << "Right column " << i << ": " << buildType_->names()[i];
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

  auto const probeTableNumColumns = probeType_->size();
  leftKeyIndices_ = std::vector<cudf::size_type>(leftKeys.size());
  for (size_t i = 0; i < leftKeyIndices_.size(); i++) {
    leftKeyIndices_[i] = static_cast<cudf::size_type>(
        probeType_->getChildIdx(leftKeys[i]->name()));
    VELOX_CHECK_LT(leftKeyIndices_[i], probeTableNumColumns);
  }
  auto const buildTableNumColumns = buildType_->size();
  rightKeyIndices_ = std::vector<cudf::size_type>(rightKeys.size());
  for (size_t i = 0; i < rightKeyIndices_.size(); i++) {
    rightKeyIndices_[i] = static_cast<cudf::size_type>(
        buildType_->getChildIdx(rightKeys[i]->name()));
    VELOX_CHECK_LT(rightKeyIndices_[i], buildTableNumColumns);
  }

  auto outputType = joinNode_->outputType();
  auto numOutputColumns = outputType->names().size();
  if (joinNode_->isLeftSemiProjectJoin()) {
    VELOX_CHECK_GE(numOutputColumns, 1);
    matchColumnOutputIndex_ =
        static_cast<int32_t>(numOutputColumns - 1);
    VELOX_CHECK_EQ(
        outputType->childAt(matchColumnOutputIndex_),
        BOOLEAN());
    --numOutputColumns;
  }

  leftColumnIndicesToGather_ = std::vector<cudf::size_type>();
  rightColumnIndicesToGather_ = std::vector<cudf::size_type>();
  leftColumnOutputIndices_ = std::vector<size_t>();
  rightColumnOutputIndices_ = std::vector<size_t>();
  for (size_t i = 0; i < numOutputColumns; i++) {
    auto const outputName = outputType->names()[i];
    if (CudfConfig::getInstance().debugEnabled) {
      VLOG(1) << "Output column " << i << ": " << outputName;
    }
    auto channel = probeType_->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      leftColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      leftColumnOutputIndices_.push_back(i);
      continue;
    }
    channel = buildType_->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      rightColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      rightColumnOutputIndices_.push_back(i);
      continue;
    }
    VELOX_FAIL(
        "Join field {} not in probe or build input",
        outputType->children()[i]);
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

  // Setup filter in case it exists
  if (joinNode_->filter()) {
    // simplify expression
    exec::ExprSet exprs({joinNode_->filter()}, operatorCtx_->execCtx());
    VELOX_CHECK_EQ(exprs.exprs().size(), 1);
    useAstFilter_ = CudfConfig::getInstance().astExpressionEnabled &&
        !containsDecimalType(exprs.exprs()[0]);
  }
}

void CudfHashJoinProbe::initialize() {
  Operator::initialize();

  if (!joinNode_->filter()) {
    return;
  }

  exec::ExprSet exprs({joinNode_->filter()}, operatorCtx_->execCtx());
  VELOX_CHECK_EQ(exprs.exprs().size(), 1);

  // Create a reusable evaluator for the filter column. This is expensive to
  // build, and the expression + input schema are stable for the lifetime of
  // the operator instance.
  std::vector<velox::RowTypePtr> filterRowTypes{probeType_, buildType_};
  filterEvaluator_ = createCudfExpression(
      exprs.exprs()[0],
      facebook::velox::type::concatRowTypes(filterRowTypes));

  // We don't need to get tables that contain conditional comparison columns
  // We'll pass the entire table. The ast will handle finding the required
  // columns. This is required because we build the ast with whole row schema
  // and the column locations in that schema translate to column locations
  // in whole tables

  if (useAstFilter_) {
    try {
      if (joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin()) {
        createAstTree(
            exprs.exprs()[0],
            tree_,
            scalars_,
            buildType_,
            probeType_,
            rightPrecomputeInstructions_,
            leftPrecomputeInstructions_);
      } else {
        createAstTree(
            exprs.exprs()[0],
            tree_,
            scalars_,
            probeType_,
            buildType_,
            leftPrecomputeInstructions_,
            rightPrecomputeInstructions_);
      }
    } catch (const VeloxException& e) {
      LOG(WARNING)
          << "CudfHashJoinProbe: AST tree creation failed for filter '"
          << joinNode_->filter()->toString()
          << "', disabling AST filter: " << e.what();
      useAstFilter_ = false;
      tree_ = {};
      scalars_.clear();
      leftPrecomputeInstructions_.clear();
      rightPrecomputeInstructions_.clear();
    }
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
  if (!joinNode_->isRightJoin() && !joinNode_->isRightSemiFilterJoin() &&
      !joinNode_->isFullJoin()) {
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

  if (joinNode_->isRightJoin() || joinNode_->isFullJoin()) {
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
  checkCudaHealth("CudfHashJoinProbe::unfilteredOutput");
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
  std::vector<cudf::column_view> joinedColViews;
  joinedColViews.reserve(joinedCols.size());
  for (const auto& col : joinedCols) {
    joinedColViews.push_back(col->view());
  }
  auto filterColumns = filterEvaluator_->eval(
      joinedColViews, stream, cudf::get_current_device_resource_ref());
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
  checkCudaHealth("CudfHashJoinProbe::filteredOutput");
  return std::make_unique<cudf::table>(std::move(joinedCols));
}

std::unique_ptr<cudf::table> CudfHashJoinProbe::filteredOutputIndices(
    cudf::table_view leftTableView,
    cudf::column_view leftIndicesCol,
    cudf::table_view rightTableView,
    cudf::column_view rightIndicesCol,
    cudf::table_view extendedLeftView,
    cudf::table_view extendedRightView,
    cudf::join_kind joinKind,
    rmm::cuda_stream_view stream) {
  // Use extended views (with precomputed columns) for filter evaluation
  auto [filteredLeftJoinIndices, filteredRightJoinIndices] =
      cudf::filter_join_indices(
          extendedLeftView,
          extendedRightView,
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
  // Use original views (without precomputed columns) for gathering output
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

  // Precompute left (probe) table columns if needed (once, outside loop)
  std::vector<ColumnOrView> leftPrecomputed;
  cudf::table_view extendedLeftView = leftTableView;
  if (joinNode_->filter() && useAstFilter_ &&
      !leftPrecomputeInstructions_.empty()) {
    try {
      auto leftColumnViews = tableViewToColumnViews(leftTableView);
      leftPrecomputed = precomputeSubexpressions(
          leftColumnViews,
          leftPrecomputeInstructions_,
          scalars_,
          probeType_,
          stream);
      extendedLeftView =
          createExtendedTableView(leftTableView, leftPrecomputed);
    } catch (const VeloxException& e) {
      LOG(WARNING)
          << "CudfHashJoinProbe::innerJoin: left precompute failed, "
          << "disabling AST filter for planNode " << joinNode_->id()
          << ": " << e.what();
      useAstFilter_ = false;
      leftPrecomputed.clear();
      extendedLeftView = leftTableView;
    }
  }

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    // Use cached precomputed columns for right (build) table
    cudf::table_view extendedRightView =
        (joinNode_->filter() && useAstFilter_ &&
         !rightPrecomputeInstructions_.empty())
        ? cachedExtendedRightViews_[i]
        : rightTableView;

    // left = probe, right = build
    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      // Make build stream wait for probe tables to become valid
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }

    // Align decimal key types between probe and build to prevent
    // "Both inputs must be of the same type" in cudf::hash_join.
    std::vector<std::unique_ptr<cudf::column>> keyCastCols;
    auto alignedProbeView = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        keyCastCols, stream);

    auto joinStream =
        buildStream_.has_value() ? buildStream_.value() : stream;

    // Pre-estimate output size to (a) allow cudf to pre-allocate
    // efficiently and (b) detect oversized outputs before the
    // allocation triggers cudaErrorInvalidValue.
    std::optional<std::size_t> estInnerSize;
    try {
      estInnerSize = hb->inner_join_size(
          alignedProbeView.select(leftKeyIndices_), joinStream);
    } catch (const std::exception& sizeErr) {
      LOG(WARNING) << "inner_join_size failed for planNode "
                   << joinNode_->id() << " batch " << i << ": "
                   << sizeErr.what();
      recoverGpuMemory();
    }

    if (estInnerSize.has_value()) {
      throwIfJoinOutputTooLarge(
          estInnerSize.value(),
          leftTableView,
          rightTableView,
          fmt::format(
              "innerJoin planNode {} batch {}", joinNode_->id(), i));
    }

    std::pair<
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>,
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>>
        joinResult;
    try {
      joinResult = hb->inner_join(
          alignedProbeView.select(leftKeyIndices_),
          estInnerSize,
          joinStream);
    } catch (const std::exception& e) {
      if (isCudaRelatedError(e)) {
        LOG(ERROR)
            << "CUDA error in inner_join for planNode " << joinNode_->id()
            << ": probe=" << leftTableView.num_rows()
            << " rows, build=" << rightTableView.num_rows()
            << " rows, probeKeyCols=" << leftKeyIndices_.size()
            << ", buildKeyCols=" << rightKeyIndices_.size()
            << ", estOutputRows="
            << (estInnerSize.has_value()
                    ? std::to_string(estInnerSize.value())
                    : "unknown")
            << ". Key types: ";
        for (size_t k = 0; k < leftKeyIndices_.size(); ++k) {
          auto pType = leftTableView.column(leftKeyIndices_[k]).type();
          auto bType = rightTableView.column(rightKeyIndices_[k]).type();
          LOG(ERROR) << "  key[" << k << "]: probe=" << static_cast<int>(pType.id())
                     << " (scale=" << pType.scale() << ")"
                     << " build=" << static_cast<int>(bType.id())
                     << " (scale=" << bType.scale() << ")";
        }
        throw;
      }
      VELOX_FAIL(
          "GPU inner_join failed (probe={} rows, build={} rows, "
          "planNode={}): {}",
          leftTableView.num_rows(),
          rightTableView.num_rows(),
          joinNode_->id(),
          e.what());
    }
    auto& [leftJoinIndices, rightJoinIndices] = joinResult;

    if (buildStream_.has_value()) {
      // Make probe stream wait for join completion before using indices
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto joinOutputRows = leftJoinIndices->size();
    auto maxBatchRows = computeMaxGatherRows(leftTableView, rightTableView);
    if (joinOutputRows > maxBatchRows) {
      LOG(INFO) << "Batching inner join gather for planNode " << joinNode_->id()
                << ": " << joinOutputRows << " output rows -> batches of "
                << maxBatchRows;
    }

    try {
      for (size_t off = 0; off < joinOutputRows; off += maxBatchRows) {
        auto batchSz = std::min(maxBatchRows, joinOutputRows - off);
        auto leftSpan = cudf::device_span<cudf::size_type const>(
            leftJoinIndices->data() + off, batchSz);
        auto rightSpan = cudf::device_span<cudf::size_type const>(
            rightJoinIndices->data() + off, batchSz);
        auto leftBatchCol = cudf::column_view{leftSpan};
        auto rightBatchCol = cudf::column_view{rightSpan};

        if (joinNode_->filter()) {
          if (useAstFilter_) {
            try {
              cudfOutputs.push_back(filteredOutputIndices(
                  leftTableView, leftBatchCol,
                  rightTableView, rightBatchCol,
                  extendedLeftView, extendedRightView,
                  cudf::join_kind::INNER_JOIN, stream));
            } catch (const std::bad_alloc&) {
              throw;
            } catch (const std::exception& astE) {
              if (isCudaRelatedError(astE)) throw;
              LOG(WARNING)
                  << "CudfHashJoinProbe::innerJoin: AST filter failed for "
                  << "planNode " << joinNode_->id()
                  << ", falling back to evaluator: " << astE.what();
              useAstFilter_ = false;
              auto filterFunc =
                  [stream](
                      std::vector<std::unique_ptr<cudf::column>>&& jc,
                      cudf::column_view fc) {
                    auto ft = std::make_unique<cudf::table>(std::move(jc));
                    auto res = cudf::apply_boolean_mask(
                        *ft, fc, stream, cudf::get_current_device_resource_ref());
                    return res->release();
                  };
              cudfOutputs.push_back(filteredOutput(
                  leftTableView, leftBatchCol,
                  rightTableView, rightBatchCol,
                  filterFunc, stream));
            }
          } else {
            auto filterFunc =
                [stream](
                    std::vector<std::unique_ptr<cudf::column>>&& jc,
                    cudf::column_view fc) {
                  auto ft = std::make_unique<cudf::table>(std::move(jc));
                  auto res = cudf::apply_boolean_mask(
                      *ft, fc, stream, cudf::get_current_device_resource_ref());
                  return res->release();
                };
            cudfOutputs.push_back(filteredOutput(
                leftTableView, leftBatchCol,
                rightTableView, rightBatchCol,
                filterFunc, stream));
          }
        } else {
          cudfOutputs.push_back(unfilteredOutput(
              leftTableView, leftBatchCol,
              rightTableView, rightBatchCol,
              stream));
        }
      }
    } catch (const std::bad_alloc&) {
      throw;
    } catch (const std::exception& e) {
      if (isCudaRelatedError(e)) throw;
      VELOX_FAIL(
          "GPU inner join gather/filter failed (joinOutput={} rows, "
          "probe={} rows, build={} rows, planNode={}): {}",
          joinOutputRows,
          leftTableView.num_rows(),
          rightTableView.num_rows(),
          joinNode_->id(),
          e.what());
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

  // Precompute left (probe) table columns if needed (once, outside loop)
  std::vector<ColumnOrView> leftPrecomputed;
  cudf::table_view extendedLeftView = leftTableView;
  if (joinNode_->filter() && useAstFilter_ &&
      !leftPrecomputeInstructions_.empty()) {
    try {
      auto leftColumnViews = tableViewToColumnViews(leftTableView);
      leftPrecomputed = precomputeSubexpressions(
          leftColumnViews,
          leftPrecomputeInstructions_,
          scalars_,
          probeType_,
          stream);
      extendedLeftView =
          createExtendedTableView(leftTableView, leftPrecomputed);
    } catch (const VeloxException& e) {
      LOG(WARNING)
          << "CudfHashJoinProbe::leftJoin: left precompute failed, "
          << "disabling AST filter for planNode " << joinNode_->id()
          << ": " << e.what();
      useAstFilter_ = false;
      leftPrecomputed.clear();
      extendedLeftView = leftTableView;
    }
  }

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    // Use cached precomputed columns for right (build) table
    cudf::table_view extendedRightView =
        (joinNode_->filter() && useAstFilter_ &&
         !rightPrecomputeInstructions_.empty())
        ? cachedExtendedRightViews_[i]
        : rightTableView;

    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    std::vector<std::unique_ptr<cudf::column>> leftKeyCastsLJ;
    auto alignedLeftLJ = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        leftKeyCastsLJ, stream);

    auto leftJoinStream =
        buildStream_.has_value() ? buildStream_.value() : stream;
    std::optional<std::size_t> estLeftSize;
    try {
      estLeftSize = hb->left_join_size(
          alignedLeftLJ.select(leftKeyIndices_), leftJoinStream);
    } catch (const std::exception& sizeErr) {
      LOG(WARNING) << "left_join_size failed for planNode "
                   << joinNode_->id() << " batch " << i << ": "
                   << sizeErr.what();
      recoverGpuMemory();
    }

    if (estLeftSize.has_value()) {
      throwIfJoinOutputTooLarge(
          estLeftSize.value(),
          leftTableView,
          rightTableView,
          fmt::format(
              "leftJoin planNode {} batch {}", joinNode_->id(), i));
    }

    std::pair<
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>,
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>>
        leftJoinResult;
    try {
      leftJoinResult = hb->left_join(
          alignedLeftLJ.select(leftKeyIndices_),
          estLeftSize,
          leftJoinStream);
    } catch (const std::exception& e) {
      if (isCudaRelatedError(e)) {
        LOG(ERROR) << "CUDA error in left_join for planNode "
                   << joinNode_->id() << ": probe="
                   << leftTableView.num_rows()
                   << " build=" << rightTableView.num_rows()
                   << " estOutput="
                   << (estLeftSize.has_value()
                           ? std::to_string(estLeftSize.value())
                           : "unknown");
        cudaGetLastError();
        throw;
      }
      throw;
    }
    auto& [leftJoinIndices, rightJoinIndices] = leftJoinResult;
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto joinOutputRows = leftJoinIndices->size();
    auto maxBatchRows = computeMaxGatherRows(leftTableView, rightTableView);
    if (joinOutputRows > maxBatchRows) {
      LOG(INFO) << "Batching left join gather for planNode " << joinNode_->id()
                << ": " << joinOutputRows << " output rows -> batches of "
                << maxBatchRows;
    }

    for (size_t off = 0; off < joinOutputRows; off += maxBatchRows) {
      auto batchSz = std::min(maxBatchRows, joinOutputRows - off);
      auto leftSpan = cudf::device_span<cudf::size_type const>(
          leftJoinIndices->data() + off, batchSz);
      auto rightSpan = cudf::device_span<cudf::size_type const>(
          rightJoinIndices->data() + off, batchSz);
      auto leftBatchCol = cudf::column_view{leftSpan};
      auto rightBatchCol = cudf::column_view{rightSpan};

      if (joinNode_->filter()) {
        if (useAstFilter_) {
          try {
            cudfOutputs.push_back(filteredOutputIndices(
                leftTableView, leftBatchCol,
                rightTableView, rightBatchCol,
                extendedLeftView, extendedRightView,
                cudf::join_kind::LEFT_JOIN, stream));
          } catch (const std::bad_alloc&) {
            throw;
          } catch (const std::exception& astE) {
            if (isCudaRelatedError(astE)) throw;
            LOG(WARNING)
                << "CudfHashJoinProbe::leftJoin: AST filter failed for "
                << "planNode " << joinNode_->id()
                << ", falling back to evaluator: " << astE.what();
            useAstFilter_ = false;
            auto filterFunc =
                [stream](
                    std::vector<std::unique_ptr<cudf::column>>&& jc,
                    cudf::column_view fc) {
                  auto ft = std::make_unique<cudf::table>(std::move(jc));
                  auto res = cudf::apply_boolean_mask(
                      *ft, fc, stream, cudf::get_current_device_resource_ref());
                  return res->release();
                };
            cudfOutputs.push_back(filteredOutput(
                leftTableView, leftBatchCol,
                rightTableView, rightBatchCol,
                filterFunc, stream));
          }
        } else {
          auto filterFunc =
              [stream](
                  std::vector<std::unique_ptr<cudf::column>>&& jc,
                  cudf::column_view fc) {
                auto ft = std::make_unique<cudf::table>(std::move(jc));
                auto res = cudf::apply_boolean_mask(
                    *ft, fc, stream, cudf::get_current_device_resource_ref());
                return res->release();
              };
          cudfOutputs.push_back(filteredOutput(
              leftTableView, leftBatchCol,
              rightTableView, rightBatchCol,
              filterFunc, stream));
        }
      } else {
        cudfOutputs.push_back(unfilteredOutput(
            leftTableView, leftBatchCol,
            rightTableView, rightBatchCol,
            stream));
      }
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

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    std::vector<std::unique_ptr<cudf::column>> keyCastsRJ;
    auto alignedProbeRJ = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        keyCastsRJ, stream);

    auto rightJoinStream =
        buildStream_.has_value() ? buildStream_.value() : stream;
    std::optional<std::size_t> estRightSize;
    try {
      estRightSize = hb->inner_join_size(
          alignedProbeRJ.select(leftKeyIndices_), rightJoinStream);
    } catch (const std::exception& sizeErr) {
      LOG(WARNING) << "inner_join_size (right) failed for planNode "
                   << joinNode_->id() << " batch " << i << ": "
                   << sizeErr.what();
      recoverGpuMemory();
    }

    if (estRightSize.has_value()) {
      throwIfJoinOutputTooLarge(
          estRightSize.value(),
          leftTableView,
          rightTableView,
          fmt::format(
              "rightJoin planNode {} batch {}", joinNode_->id(), i));
    }

    std::pair<
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>,
        std::unique_ptr<rmm::device_uvector<cudf::size_type>>>
        rightJoinResult;
    try {
      rightJoinResult = hb->inner_join(
          alignedProbeRJ.select(leftKeyIndices_),
          estRightSize,
          rightJoinStream);
    } catch (const std::exception& e) {
      if (isCudaRelatedError(e)) {
        LOG(ERROR) << "CUDA error in inner_join (right) for planNode "
                   << joinNode_->id() << ": probe="
                   << leftTableView.num_rows()
                   << " build=" << rightTableView.num_rows()
                   << " estOutput="
                   << (estRightSize.has_value()
                           ? std::to_string(estRightSize.value())
                           : "unknown");
        cudaGetLastError();
        throw;
      }
      throw;
    }
    auto& [leftJoinIndices, rightJoinIndices] = rightJoinResult;
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto joinOutputRows = leftJoinIndices->size();
    auto maxBatchRows = computeMaxGatherRows(leftTableView, rightTableView);
    if (joinOutputRows > maxBatchRows) {
      LOG(INFO) << "Batching right join gather for planNode " << joinNode_->id()
                << ": " << joinOutputRows << " output rows -> batches of "
                << maxBatchRows;
    }

    // Update matched flags with FULL indices before batching the gather.
    // cudf::contains operates on the full index array which is lightweight
    // (no column materialization), so it doesn't trigger oversized allocs.
    if (!joinNode_->filter() && rightTableView.num_rows() > 0) {
      auto rightIdxCol = cudf::column_view{
          cudf::device_span<cudf::size_type const>{*rightJoinIndices}};

      auto n = rightTableView.num_rows();
      auto rowIndices = cudf::sequence(
          n,
          cudf::numeric_scalar<cudf::size_type>(0, true, stream),
          cudf::numeric_scalar<cudf::size_type>(1, true, stream),
          stream,
          cudf::get_current_device_resource_ref());

      auto matchedInBatch = cudf::contains(rightIdxCol, rowIndices->view());

      auto updatedFlags = cudf::binary_operation(
          rightMatchedFlags_[i]->view(),
          matchedInBatch->view(),
          cudf::binary_operator::BITWISE_OR,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          cudf::get_current_device_resource_ref());
      rightMatchedFlags_[i] = std::move(updatedFlags);
    }

    for (size_t off = 0; off < joinOutputRows; off += maxBatchRows) {
      auto batchSz = std::min(maxBatchRows, joinOutputRows - off);
      auto leftSpan = cudf::device_span<cudf::size_type const>(
          leftJoinIndices->data() + off, batchSz);
      auto rightSpan = cudf::device_span<cudf::size_type const>(
          rightJoinIndices->data() + off, batchSz);
      auto leftBatchCol = cudf::column_view{leftSpan};
      auto rightBatchCol = cudf::column_view{rightSpan};

      if (joinNode_->filter()) {
        auto& rightMatchedFlags = rightMatchedFlags_[i];
        auto numBuildRows = rightTableView.num_rows();
        auto batchRightSpan = rightSpan;
        auto filterFunc =
            [&rightMatchedFlags, batchRightSpan, numBuildRows, stream](
                std::vector<std::unique_ptr<cudf::column>>&& joinedCols,
                cudf::column_view filterColumn) {
              auto filterTable =
                  std::make_unique<cudf::table>(std::move(joinedCols));
              auto filteredTable =
                  cudf::apply_boolean_mask(*filterTable, filterColumn, stream);
              joinedCols = filteredTable->release();

              auto rightIdxCol = cudf::column_view{batchRightSpan};
              auto filteredIdxTable = cudf::apply_boolean_mask(
                  cudf::table_view{std::vector<cudf::column_view>{rightIdxCol}},
                  filterColumn,
                  stream);
              auto filteredCols = filteredIdxTable->release();
              auto filteredRightIdxCol = std::move(filteredCols[0]);

              if (numBuildRows > 0) {
                auto rowIndices = cudf::sequence(
                    numBuildRows,
                    cudf::numeric_scalar<cudf::size_type>(0, true, stream),
                    cudf::numeric_scalar<cudf::size_type>(1, true, stream),
                    stream,
                    cudf::get_current_device_resource_ref());

                auto matchedInBatch = cudf::contains(
                    filteredRightIdxCol->view(), rowIndices->view());

                auto updatedFlags = cudf::binary_operation(
                    rightMatchedFlags->view(),
                    matchedInBatch->view(),
                    cudf::binary_operator::BITWISE_OR,
                    cudf::data_type{cudf::type_id::BOOL8},
                    stream,
                    cudf::get_current_device_resource_ref());
                rightMatchedFlags = std::move(updatedFlags);
              }
              return std::move(joinedCols);
            };
        cudfOutputs.push_back(filteredOutput(
            leftTableView, leftBatchCol,
            rightTableView, rightBatchCol,
            filterFunc, stream));
      } else {
        cudfOutputs.push_back(unfilteredOutput(
            leftTableView, leftBatchCol,
            rightTableView, rightBatchCol,
            stream));
      }
    }
  }
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>> CudfHashJoinProbe::fullJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  auto& hbs = hashObject_.value().second;

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();
    auto& hb = hbs[i];

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    VELOX_CHECK_NOT_NULL(hb);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
    }
    std::vector<std::unique_ptr<cudf::column>> keyCastsFJ;
    auto alignedLeftFJ = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        keyCastsFJ, stream);

    auto fullJoinStream =
        buildStream_.has_value() ? buildStream_.value() : stream;
    std::optional<std::size_t> estFullSize;
    try {
      estFullSize = hb->left_join_size(
          alignedLeftFJ.select(leftKeyIndices_), fullJoinStream);
    } catch (const std::exception& sizeErr) {
      LOG(WARNING) << "left_join_size (full) failed for planNode "
                   << joinNode_->id() << " batch " << i << ": "
                   << sizeErr.what();
      recoverGpuMemory();
    }

    auto [leftJoinIndices, rightJoinIndices] = hb->left_join(
        alignedLeftFJ.select(leftKeyIndices_),
        estFullSize,
        fullJoinStream);
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(buildStream_.value()).waitOn(stream);
    }

    auto joinOutputRows = leftJoinIndices->size();
    auto maxBatchRows = computeMaxGatherRows(leftTableView, rightTableView);
    if (joinOutputRows > maxBatchRows) {
      LOG(INFO) << "Batching full join gather for planNode " << joinNode_->id()
                << ": " << joinOutputRows << " output rows -> batches of "
                << maxBatchRows;
    }

    // Update matched flags with FULL indices before batching the gather.
    if (!joinNode_->filter() && rightTableView.num_rows() > 0) {
      auto rightIdxCol = cudf::column_view{
          cudf::device_span<cudf::size_type const>{*rightJoinIndices}};

      auto n = rightTableView.num_rows();
      auto rowIndices = cudf::sequence(
          n,
          cudf::numeric_scalar<cudf::size_type>(0, true, stream),
          cudf::numeric_scalar<cudf::size_type>(1, true, stream),
          stream,
          cudf::get_current_device_resource_ref());

      auto matchedInBatch = cudf::contains(rightIdxCol, rowIndices->view());

      auto updatedFlags = cudf::binary_operation(
          rightMatchedFlags_[i]->view(),
          matchedInBatch->view(),
          cudf::binary_operator::BITWISE_OR,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          cudf::get_current_device_resource_ref());
      rightMatchedFlags_[i] = std::move(updatedFlags);
    }

    if (joinNode_->filter()) {
      // Process filter + gather in batches over the raw join indices.
      for (size_t off = 0; off < joinOutputRows; off += maxBatchRows) {
        auto batchSz = std::min(maxBatchRows, joinOutputRows - off);
        auto leftSpan = cudf::device_span<cudf::size_type const>(
            leftJoinIndices->data() + off, batchSz);
        auto rightSpan = cudf::device_span<cudf::size_type const>(
            rightJoinIndices->data() + off, batchSz);
        auto leftBatchCol = cudf::column_view{leftSpan};
        auto rightBatchCol = cudf::column_view{rightSpan};

        auto [filteredLeftJoinIndices, filteredRightJoinIndices] =
            cudf::filter_join_indices(
                leftTableView, rightTableView,
                leftBatchCol, rightBatchCol,
                tree_.back(), cudf::join_kind::LEFT_JOIN, stream);

        auto& rightMatchedFlags = rightMatchedFlags_[i];
        auto filteredRightIndicesSpan =
            cudf::device_span<cudf::size_type const>{
                *filteredRightJoinIndices};
        auto filteredRightIdxCol =
            cudf::column_view{filteredRightIndicesSpan};

        if (rightTableView.num_rows() > 0) {
          auto n = rightTableView.num_rows();
          auto rowIndices = cudf::sequence(
              n,
              cudf::numeric_scalar<cudf::size_type>(0, true, stream),
              cudf::numeric_scalar<cudf::size_type>(1, true, stream),
              stream,
              cudf::get_current_device_resource_ref());

          auto matchedInBatch =
              cudf::contains(filteredRightIdxCol, rowIndices->view());

          auto updatedFlags = cudf::binary_operation(
              rightMatchedFlags->view(),
              matchedInBatch->view(),
              cudf::binary_operator::BITWISE_OR,
              cudf::data_type{cudf::type_id::BOOL8},
              stream,
              cudf::get_current_device_resource_ref());
          rightMatchedFlags = std::move(updatedFlags);
        }

        auto filteredLeftIndicesSpan =
            cudf::device_span<cudf::size_type const>{
                *filteredLeftJoinIndices};
        auto filteredLeftIndicesCol =
            cudf::column_view{filteredLeftIndicesSpan};
        auto filteredRightIndicesCol =
            cudf::column_view{filteredRightIndicesSpan};
        cudfOutputs.push_back(unfilteredOutput(
            leftTableView, filteredLeftIndicesCol,
            rightTableView, filteredRightIndicesCol,
            stream));
      }
    } else {
      for (size_t off = 0; off < joinOutputRows; off += maxBatchRows) {
        auto batchSz = std::min(maxBatchRows, joinOutputRows - off);
        auto leftSpan = cudf::device_span<cudf::size_type const>(
            leftJoinIndices->data() + off, batchSz);
        auto rightSpan = cudf::device_span<cudf::size_type const>(
            rightJoinIndices->data() + off, batchSz);
        cudfOutputs.push_back(unfilteredOutput(
            leftTableView, cudf::column_view{leftSpan},
            rightTableView, cudf::column_view{rightSpan},
            stream));
      }
    }
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

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    std::vector<std::unique_ptr<cudf::column>> keyCastsLSF;
    auto alignedLeft = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        keyCastsLSF, stream);

    std::unique_ptr<rmm::device_uvector<cudf::size_type>> leftJoinIndices;

    if (joinNode_->filter()) {
      if (!useAstFilter_) {
        VELOX_NYI("Join filter requires AST for semi joins");
      }
      leftJoinIndices = cudf::mixed_left_semi_join(
          alignedLeft.select(leftKeyIndices_),
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
          alignedLeft.select(leftKeyIndices_),
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
CudfHashJoinProbe::leftSemiProjectJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;
  auto& rightTables = hashObject_.value().first;
  auto leftNumRows = leftTableView.num_rows();

  if (leftNumRows == 0) {
    return cudfOutputs;
  }

  auto falseScalar =
      cudf::numeric_scalar<bool>(false, true, stream);
  auto matchFlags = cudf::make_column_from_scalar(
      falseScalar,
      leftNumRows,
      stream,
      cudf::get_current_device_resource_ref());

  auto rowIndices = cudf::sequence(
      leftNumRows,
      cudf::numeric_scalar<cudf::size_type>(0, true, stream),
      cudf::numeric_scalar<cudf::size_type>(1, true, stream),
      stream,
      cudf::get_current_device_resource_ref());

  for (auto i = 0; i < rightTables.size(); i++) {
    auto rightTableView = rightTables[i]->view();

    if (rightTableView.num_rows() == 0) {
      continue;
    }

    std::vector<std::unique_ptr<cudf::column>> keyCastsLSP;
    auto alignedLeft = alignProbeKeyTypes(
        leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
        keyCastsLSP, stream);

    std::unique_ptr<rmm::device_uvector<cudf::size_type>>
        leftJoinIndices;

    if (joinNode_->filter()) {
      leftJoinIndices = cudf::mixed_left_semi_join(
          alignedLeft.select(leftKeyIndices_),
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
          alignedLeft.select(leftKeyIndices_),
          stream,
          cudf::get_current_device_resource_ref());
    }

    if (leftJoinIndices->size() > 0) {
      auto leftIdxCol = cudf::column_view{
          cudf::device_span<cudf::size_type const>{
              *leftJoinIndices}};
      auto matchedInBatch =
          cudf::contains(leftIdxCol, rowIndices->view());
      auto updated = cudf::binary_operation(
          matchFlags->view(),
          matchedInBatch->view(),
          cudf::binary_operator::BITWISE_OR,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          cudf::get_current_device_resource_ref());
      matchFlags = std::move(updated);
    }
  }

  if (joinNode_->isNullAware()) {
    auto mr = cudf::get_current_device_resource_ref();
    bool buildIsEmpty = true;
    bool buildHasNullKeys = false;
    for (const auto& rt : rightTables) {
      if (rt->view().num_rows() > 0) {
        buildIsEmpty = false;
        if (cudf::has_nulls(
                rt->view().select(rightKeyIndices_))) {
          buildHasNullKeys = true;
        }
      }
    }

    // x IN (empty set) = false for any x, including null.
    if (!buildIsEmpty) {
      auto trueScalar =
          cudf::numeric_scalar<bool>(true, true, stream);
      auto probeKeyNotNull = cudf::make_column_from_scalar(
          trueScalar, leftNumRows, stream, mr);
      for (auto ki : leftKeyIndices_) {
        auto keyCol = leftTableView.column(ki);
        if (keyCol.has_nulls()) {
          auto isValid =
              cudf::is_valid(keyCol, stream, mr);
          probeKeyNotNull = cudf::binary_operation(
              probeKeyNotNull->view(),
              isValid->view(),
              cudf::binary_operator::BITWISE_AND,
              cudf::data_type{cudf::type_id::BOOL8},
              stream,
              mr);
        }
      }

      std::unique_ptr<cudf::column> validMask;
      if (buildHasNullKeys) {
        validMask = cudf::binary_operation(
            probeKeyNotNull->view(),
            matchFlags->view(),
            cudf::binary_operator::BITWISE_AND,
            cudf::data_type{cudf::type_id::BOOL8},
            stream,
            mr);
      } else {
        validMask = std::move(probeKeyNotNull);
      }

      auto nullScalar =
          cudf::numeric_scalar<bool>(false, false, stream);
      matchFlags = cudf::copy_if_else(
          matchFlags->view(),
          nullScalar,
          validMask->view(),
          stream,
          mr);
    }
  }

  auto leftInput =
      leftTableView.select(leftColumnIndicesToGather_);
  std::vector<std::unique_ptr<cudf::column>> outCols(
      outputType_->size());
  for (size_t j = 0; j < leftColumnOutputIndices_.size(); ++j) {
    auto outIdx = leftColumnOutputIndices_[j];
    outCols[outIdx] = std::make_unique<cudf::column>(
        leftInput.column(j),
        stream,
        cudf::get_current_device_resource_ref());
  }
  outCols[matchColumnOutputIndex_] = std::move(matchFlags);

  if (buildStream_.has_value()) {
    cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
  }
  stream.synchronize();
  checkCudaHealth("CudfHashJoinProbe::leftSemiProjectJoin");
  cudfOutputs.push_back(
      std::make_unique<cudf::table>(std::move(outCols)));
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>>
CudfHashJoinProbe::rightSemiFilterJoin(
    cudf::table_view leftTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;

  auto& rightTables = hashObject_.value().first;
  VELOX_CHECK(
      !rightTables.empty(),
      "rightTables is empty in rightSemiFilterJoin, planNode={}",
      joinNode_->id());
  auto rightTableView = rightTables[0]->view();

  VELOX_CHECK_EQ(
      rightTables.size(),
      1,
      "Multiple right tables not yet supported for rightSemiFilterJoin");

  if (rightTableView.num_rows() == 0 || leftTableView.num_rows() == 0) {
    return cudfOutputs;
  }

  std::vector<std::unique_ptr<cudf::column>> keyCastsRSF;
  auto alignedRight = alignProbeKeyTypes(
      rightTableView, rightKeyIndices_, leftTableView, leftKeyIndices_,
      keyCastsRSF, stream);

  std::unique_ptr<rmm::device_uvector<cudf::size_type>> rightJoinIndices;
  if (joinNode_->filter()) {
    if (!useAstFilter_) {
      VELOX_NYI("Join filter requires AST for semi joins");
    }
    rightJoinIndices = cudf::mixed_left_semi_join(
        alignedRight.select(rightKeyIndices_),
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
        alignedRight.select(rightKeyIndices_),
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
  auto mr = cudf::get_current_device_resource_ref();

  VELOX_CHECK_EQ(
      rightTables.size(),
      1,
      "Multiple right tables not yet supported for antiJoin");

  auto rightTableView = rightTables[0]->view();

  if (joinNode_->isNullAware() && joinNode_->filter()) {
    return nullAwareAntiJoinWithFilter(
        leftTableViewParam, rightTableView, stream);
  }

  std::unique_ptr<cudf::table> modifiedLeftTable;
  cudf::table_view leftTableView = leftTableViewParam;

  if (joinNode_->isNullAware() && !joinNode_->filter()) {
    auto const leftTableHasNulls =
        cudf::has_nulls(
            leftTableViewParam.select(leftKeyIndices_));
    auto const rightTableHasNulls =
        cudf::has_nulls(
            rightTableView.select(rightKeyIndices_));
    if (rightTables[0]->num_rows() > 0 &&
        !rightTableHasNulls && leftTableHasNulls) {
      modifiedLeftTable = cudf::drop_nulls(
          leftTableViewParam, leftKeyIndices_, stream);
      leftTableView = modifiedLeftTable->view();
    }
  }

  std::vector<std::unique_ptr<cudf::column>> keyCastsAnti;
  auto alignedLeft = alignProbeKeyTypes(
      leftTableView, leftKeyIndices_, rightTableView, rightKeyIndices_,
      keyCastsAnti, stream);

  std::unique_ptr<rmm::device_uvector<cudf::size_type>>
      leftJoinIndices;
  if (joinNode_->filter()) {
    if (!useAstFilter_) {
      VELOX_NYI("Join filter requires AST for anti joins");
    }
    leftJoinIndices = cudf::mixed_left_anti_join(
        alignedLeft.select(leftKeyIndices_),
        rightTableView.select(rightKeyIndices_),
        leftTableView,
        rightTableView,
        tree_.back(),
        cudf::null_equality::UNEQUAL,
        stream,
        mr);
  } else {
    auto const rightTableHasNulls =
        cudf::has_nulls(
            rightTableView.select(rightKeyIndices_));
    if (joinNode_->isNullAware() && rightTableHasNulls) {
      leftJoinIndices =
          std::make_unique<rmm::device_uvector<cudf::size_type>>(
              0, stream, mr);
    } else {
      cudf::filtered_join filter_join(
          rightTableView.select(rightKeyIndices_),
          cudf::null_equality::UNEQUAL,
          cudf::set_as_build_table::RIGHT,
          stream);
      leftJoinIndices = filter_join.anti_join(
          alignedLeft.select(leftKeyIndices_),
          stream,
          mr);
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

std::vector<std::unique_ptr<cudf::table>>
CudfHashJoinProbe::nullAwareAntiJoinWithFilter(
    cudf::table_view leftTableViewParam,
    cudf::table_view rightTableView,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;
  auto mr = cudf::get_current_device_resource_ref();

  auto leftNonNullTable =
      cudf::drop_nulls(leftTableViewParam, leftKeyIndices_, stream);
  auto leftNonNull = leftNonNullTable->view();
  auto leftN = leftNonNull.num_rows();

  auto buildHasNullKeys =
      cudf::has_nulls(rightTableView.select(rightKeyIndices_));

  // Align DECIMAL key types between probe and build
  std::vector<std::unique_ptr<cudf::column>> keyCastsNAAJ;
  auto alignedLeftNonNull = alignProbeKeyTypes(
      leftNonNull, leftKeyIndices_, rightTableView, rightKeyIndices_,
      keyCastsNAAJ, stream);

  std::unique_ptr<cudf::column> rowIndices;
  std::unique_ptr<cudf::column> candidateMask;
  if (leftN > 0) {
    rowIndices = cudf::sequence(
        leftN,
        cudf::numeric_scalar<cudf::size_type>(0, true, stream),
        cudf::numeric_scalar<cudf::size_type>(1, true, stream),
        stream,
        mr);

    candidateMask = cudf::make_column_from_scalar(
        cudf::numeric_scalar<bool>(true, true, stream),
        leftN,
        stream,
        mr);
  }

  // Exclude left rows matching non-null-key build rows via
  // key equality + filter. With UNEQUAL null equality, null
  // build keys are invisible.
  if (leftN > 0 && rightTableView.num_rows() > 0) {
    std::unique_ptr<rmm::device_uvector<cudf::size_type>>
        antiIndices;
    if (buildHasNullKeys) {
      // Extract non-null-key build rows
      auto rightNonNull = cudf::drop_nulls(
          rightTableView, rightKeyIndices_, stream);
      if (rightNonNull->num_rows() > 0) {
        std::vector<std::unique_ptr<cudf::column>> keyCastsNNR;
        auto alignedLeftNN = alignProbeKeyTypes(
            leftNonNull, leftKeyIndices_,
            rightNonNull->view(), rightKeyIndices_,
            keyCastsNNR, stream);
        antiIndices = cudf::mixed_left_anti_join(
            alignedLeftNN.select(leftKeyIndices_),
            rightNonNull->view().select(rightKeyIndices_),
            leftNonNull,
            rightNonNull->view(),
            tree_.back(),
            cudf::null_equality::UNEQUAL,
            stream,
            mr);
      }
    } else {
      antiIndices = cudf::mixed_left_anti_join(
          alignedLeftNonNull.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftNonNull,
          rightTableView,
          tree_.back(),
          cudf::null_equality::UNEQUAL,
          stream,
          mr);
    }
    if (antiIndices && antiIndices->size() > 0) {
      auto antiCol = cudf::column_view{
          cudf::device_span<cudf::size_type const>{
              *antiIndices}};
      auto inAnti =
          cudf::contains(antiCol, rowIndices->view());
      candidateMask = cudf::binary_operation(
          candidateMask->view(),
          inAnti->view(),
          cudf::binary_operator::BITWISE_AND,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          mr);
    } else if (!antiIndices || antiIndices->size() == 0) {
      if (!buildHasNullKeys ||
          cudf::drop_nulls(
              rightTableView, rightKeyIndices_, stream)
                  ->num_rows() > 0) {
        // Anti-join returned nothing: all left rows matched.
        // Unless no non-null build rows existed.
        auto falseScalar =
            cudf::numeric_scalar<bool>(false, true, stream);
        candidateMask = cudf::make_column_from_scalar(
            falseScalar, leftN, stream, mr);
      }
    }
  }

  // Exclude left rows matching any null-key build row via
  // filter. Use dummy constant keys so every pair passes key
  // equality, letting the filter decide.
  if (leftN > 0 && buildHasNullKeys &&
      rightTableView.num_rows() > 0) {
    // Build per-row "any key is null" mask for right table
    auto rightN = rightTableView.num_rows();
    auto anyKeyNull = cudf::make_column_from_scalar(
        cudf::numeric_scalar<bool>(false, true, stream),
        rightN,
        stream,
        mr);
    for (auto keyIdx : rightKeyIndices_) {
      auto col = rightTableView.column(keyIdx);
      if (!col.has_nulls()) {
        continue;
      }
      auto nullScalar =
          cudf::make_default_constructed_scalar(
              col.type(), stream, mr);
      auto isNull = cudf::binary_operation(
          col,
          *nullScalar,
          cudf::binary_operator::NULL_EQUALS,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          mr);
      anyKeyNull = cudf::binary_operation(
          anyKeyNull->view(),
          isNull->view(),
          cudf::binary_operator::BITWISE_OR,
          cudf::data_type{cudf::type_id::BOOL8},
          stream,
          mr);
    }

    auto rightNullKeys = cudf::apply_boolean_mask(
        rightTableView, anyKeyNull->view(), stream);
    auto nullKeyN = rightNullKeys->num_rows();

    if (nullKeyN > 0) {
      // Dummy constant keys: every (left, rightNull) pair
      // passes key equality so the filter is evaluated on all
      // cross-product pairs.
      auto zeroScalar = cudf::numeric_scalar<cudf::size_type>(
          0, true, stream);
      auto leftDummyKey = cudf::make_column_from_scalar(
          zeroScalar, leftN, stream, mr);
      auto rightDummyKey = cudf::make_column_from_scalar(
          zeroScalar, nullKeyN, stream, mr);

      auto semiIndices = cudf::mixed_left_semi_join(
          cudf::table_view({leftDummyKey->view()}),
          cudf::table_view({rightDummyKey->view()}),
          leftNonNull,
          rightNullKeys->view(),
          tree_.back(),
          cudf::null_equality::UNEQUAL,
          stream,
          mr);

      if (semiIndices->size() > 0) {
        auto semiCol = cudf::column_view{
            cudf::device_span<cudf::size_type const>{
                *semiIndices}};
        auto inSemi =
            cudf::contains(semiCol, rowIndices->view());
        auto notInSemi = cudf::unary_operation(
            inSemi->view(),
            cudf::unary_operator::NOT,
            stream);
        candidateMask = cudf::binary_operation(
            candidateMask->view(),
            notInSemi->view(),
            cudf::binary_operator::BITWISE_AND,
            cudf::data_type{cudf::type_id::BOOL8},
            stream,
            mr);
      }
    }
  }

  // Collect surviving non-null-keyed rows
  std::unique_ptr<cudf::table> nonNullResult;
  if (leftN > 0) {
    auto leftInput =
        leftNonNull.select(leftColumnIndicesToGather_);
    nonNullResult = cudf::apply_boolean_mask(
        leftInput, candidateMask->view(), stream);
  }

  // Handle null-keyed probe rows.
  // null NOT IN (empty) = true  -> keep
  // null NOT IN (non-empty where filter passes) = null -> exclude
  // null NOT IN (non-empty where NO filter passes) = true -> keep
  std::unique_ptr<cudf::table> nullKeyResult;
  auto totalLeft = leftTableViewParam.num_rows();
  if (totalLeft > leftN) {
    // Extract rows where any key column is null
    auto trueScalar =
        cudf::numeric_scalar<bool>(true, true, stream);
    auto hasNullKey = cudf::make_column_from_scalar(
        cudf::numeric_scalar<bool>(false, true, stream),
        totalLeft, stream, mr);
    for (auto ki : leftKeyIndices_) {
      auto col = leftTableViewParam.column(ki);
      if (col.has_nulls()) {
        auto isNull = cudf::is_null(col, stream, mr);
        hasNullKey = cudf::binary_operation(
            hasNullKey->view(),
            isNull->view(),
            cudf::binary_operator::BITWISE_OR,
            cudf::data_type{cudf::type_id::BOOL8},
            stream, mr);
      }
    }
    auto leftNullKeyTable = cudf::apply_boolean_mask(
        leftTableViewParam, hasNullKey->view(), stream);
    auto nullN = leftNullKeyTable->num_rows();

    if (nullN > 0 && rightTableView.num_rows() == 0) {
      // Build is empty: all null-keyed rows survive
      nullKeyResult = std::make_unique<cudf::table>(
          leftNullKeyTable->view().select(
              leftColumnIndicesToGather_),
          stream, mr);
    } else if (nullN > 0 && rightTableView.num_rows() > 0) {
      // Cross-join anti: for each null-keyed probe row,
      // check if ANY build row passes the filter.
      auto buildN = rightTableView.num_rows();
      auto zeroScalar = cudf::numeric_scalar<cudf::size_type>(
          0, true, stream);
      auto dummyL = cudf::make_column_from_scalar(
          zeroScalar, nullN, stream, mr);
      auto dummyR = cudf::make_column_from_scalar(
          zeroScalar, buildN, stream, mr);

      auto antiIdx = cudf::mixed_left_anti_join(
          cudf::table_view({dummyL->view()}),
          cudf::table_view({dummyR->view()}),
          leftNullKeyTable->view(),
          rightTableView,
          tree_.back(),
          cudf::null_equality::UNEQUAL,
          stream, mr);

      if (antiIdx->size() > 0) {
        auto idxCol = cudf::column_view{
            cudf::device_span<cudf::size_type const>{
                *antiIdx}};
        nullKeyResult = cudf::gather(
            leftNullKeyTable->view().select(
                leftColumnIndicesToGather_),
            idxCol, cudf::out_of_bounds_policy::DONT_CHECK,
            stream, mr);
      }
    }
  }

  // Concatenate non-null and null-key results
  std::vector<cudf::table_view> toConcat;
  if (nonNullResult && nonNullResult->num_rows() > 0) {
    toConcat.push_back(nonNullResult->view());
  }
  if (nullKeyResult && nullKeyResult->num_rows() > 0) {
    toConcat.push_back(nullKeyResult->view());
  }

  if (toConcat.empty()) {
    // Return an empty table with correct schema
    std::vector<std::unique_ptr<cudf::column>> emptyCols;
    for (size_t j = 0; j < leftColumnOutputIndices_.size(); ++j) {
      auto srcIdx = leftColumnIndicesToGather_[j];
      auto type = leftTableViewParam.column(srcIdx).type();
      emptyCols.push_back(
          cudf::make_empty_column(type));
    }
    std::vector<std::unique_ptr<cudf::column>> outCols(
        outputType_->size());
    for (size_t j = 0; j < leftColumnOutputIndices_.size(); ++j) {
      outCols[leftColumnOutputIndices_[j]] =
          std::move(emptyCols[j]);
    }
    if (buildStream_.has_value()) {
      cudaEvent_->recordFrom(stream).waitOn(
          buildStream_.value());
    }
    stream.synchronize();
    checkCudaHealth("CudfHashJoinProbe::rightJoin empty-match path");
    cudfOutputs.push_back(
        std::make_unique<cudf::table>(std::move(outCols)));
    return cudfOutputs;
  }

  std::unique_ptr<cudf::table> finalResult;
  if (toConcat.size() == 1) {
    finalResult = std::make_unique<cudf::table>(
        toConcat[0], stream, mr);
  } else {
    finalResult =
        cudf::concatenate(toConcat, stream, mr);
  }

  auto resultCols = finalResult->release();
  std::vector<std::unique_ptr<cudf::column>> outCols(
      outputType_->size());
  for (size_t j = 0; j < leftColumnOutputIndices_.size(); ++j) {
    outCols[leftColumnOutputIndices_[j]] =
        std::move(resultCols[j]);
  }

  if (buildStream_.has_value()) {
    cudaEvent_->recordFrom(stream).waitOn(buildStream_.value());
  }
  stream.synchronize();
  checkCudaHealth("CudfHashJoinProbe::rightJoin");
  cudfOutputs.push_back(
      std::make_unique<cudf::table>(std::move(outCols)));
  return cudfOutputs;
}

std::vector<std::unique_ptr<cudf::table>>
CudfHashJoinProbe::joinWithChunkedBuild(
    cudf::table_view probeSlice,
    rmm::cuda_stream_view stream) {
  std::vector<std::unique_ptr<cudf::table>> results;

  if (!hashObject_.has_value()) {
    return results;
  }

  auto& origRT = hashObject_.value().first;

  size_t gpuFree = freeGpuMemoryBytes();
  size_t targetChunkBytes = std::max(size_t(64 << 20), gpuFree / 8);

  auto savedHash = std::move(hashObject_);
  auto savedAst = useAstFilter_;
  auto savedRP = std::move(cachedRightPrecomputed_);
  auto savedEV = std::move(cachedExtendedRightViews_);
  auto savedBS = buildStream_;
  auto savedFlags = std::move(rightMatchedFlags_);

  auto restoreState = [&]() {
    hashObject_ = std::move(savedHash);
    useAstFilter_ = savedAst;
    cachedRightPrecomputed_ = std::move(savedRP);
    cachedExtendedRightViews_ = std::move(savedEV);
    buildStream_ = savedBS;
    rightMatchedFlags_ = std::move(savedFlags);
  };

  useAstFilter_ = false;
  cachedRightPrecomputed_.clear();
  cachedExtendedRightViews_.clear();
  buildStream_ = std::nullopt;

  try {
    for (size_t bi = 0; bi < origRT.size(); ++bi) {
      auto buildView = origRT[bi]->view();
      if (buildView.num_rows() == 0) {
        continue;
      }

      size_t bytesPerRow = 0;
      for (int c = 0; c < buildView.num_columns(); ++c) {
        auto dt = buildView.column(c).type();
        bytesPerRow += cudf::is_fixed_width(dt) ? cudf::size_of(dt) : 32;
      }
      bytesPerRow = std::max(bytesPerRow, size_t(8));

      auto maxChunkRows = static_cast<cudf::size_type>(std::min(
          static_cast<size_t>(buildView.num_rows()),
          std::max(size_t(1024), targetChunkBytes / (bytesPerRow * 3))));

      if (maxChunkRows >= buildView.num_rows()) {
        maxChunkRows = std::max(
            cudf::size_type(1024),
            static_cast<cudf::size_type>(buildView.num_rows() / 4));
      }

      std::vector<cudf::size_type> splitPts;
      for (cudf::size_type i = maxChunkRows; i < buildView.num_rows();
           i += maxChunkRows) {
        splitPts.push_back(i);
      }
      auto buildChunks = cudf::split(buildView, splitPts, stream);

      LOG(INFO) << "Build-side chunking for planNode " << joinNode_->id()
                << ": " << buildView.num_rows() << " build rows -> "
                << buildChunks.size() << " chunks of ~" << maxChunkRows
                << " (probeRows=" << probeSlice.num_rows()
                << ", gpuFreeMB=" << (gpuFree >> 20) << ")";

      for (size_t ci = 0; ci < buildChunks.size(); ++ci) {
        auto& bChunk = buildChunks[ci];
        if (bChunk.num_rows() == 0) {
          continue;
        }

        auto chunkTable = std::make_shared<cudf::table>(
            bChunk, stream, cudf::get_current_device_resource_ref());

        std::shared_ptr<cudf::hash_join> chunkHJ;
        try {
          chunkHJ = std::make_shared<cudf::hash_join>(
              chunkTable->view().select(rightKeyIndices_),
              cudf::null_equality::UNEQUAL,
              stream);
        } catch (const std::exception& e) {
          LOG(WARNING) << "Build chunk " << ci << "/" << buildChunks.size()
                       << " hash table build failed for planNode "
                       << joinNode_->id() << ": " << e.what();
          recoverGpuMemory();
          continue;
        }

        std::vector<std::shared_ptr<cudf::table>> tempRT = {chunkTable};
        std::vector<std::shared_ptr<cudf::hash_join>> tempHB = {chunkHJ};
        hashObject_ = std::make_optional(
            std::make_pair(std::move(tempRT), std::move(tempHB)));

        if (joinNode_->isRightJoin() || joinNode_->isFullJoin()) {
          rightMatchedFlags_.clear();
          auto n = chunkTable->num_rows();
          auto falseScalar =
              cudf::numeric_scalar<bool>(false, true, stream);
          rightMatchedFlags_.push_back(cudf::make_column_from_scalar(
              falseScalar,
              n,
              stream,
              cudf::get_current_device_resource_ref()));
        }

        try {
          auto chunkResults = innerJoin(probeSlice, stream);
          for (auto& r : chunkResults) {
            results.push_back(std::move(r));
          }
        } catch (const std::exception& e) {
          LOG(WARNING) << "Build chunk " << ci << "/" << buildChunks.size()
                       << " join failed for planNode " << joinNode_->id()
                       << ": " << e.what();
          recoverGpuMemory();
        }

        hashObject_.reset();
        recoverGpuMemory();
      }
    }
  } catch (...) {
    restoreState();
    throw;
  }

  restoreState();
  return results;
}

RowVectorPtr CudfHashJoinProbe::getOutput() {
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(2) << "Calling CudfHashJoinProbe::getOutput";
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  GpuGuard gpuGuard;

  if (finished_ or !hashObject_.has_value()) {
    return nullptr;
  }

  if (!pendingJoinOutputs_.empty()) {
    auto tbl = std::move(pendingJoinOutputs_.back());
    pendingJoinOutputs_.pop_back();
    auto stream = cudfGlobalStreamPool().get_stream();
    if (pendingJoinOutputs_.empty()) {
      finished_ = noMoreInput_ && !joinNode_->isRightJoin() &&
          !joinNode_->isFullJoin();
    }
    auto const size = tbl->num_rows();
    if (tbl->num_columns() == 0 || size == 0) {
      return nullptr;
    }
    return std::make_shared<CudfVector>(
        pool(), outputType_, size, std::move(tbl), stream);
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
        try {
          auto tbl =
              getConcatenatedTable(accumulatedProbeInputs_, probeType, stream);
          input_ = std::make_shared<CudfVector>(
              operatorCtx_->pool(),
              probeType,
              tbl->num_rows(),
              std::move(tbl),
              stream);
        } catch (const std::exception& e) {
          if (!isCudaRelatedError(e)) {
            throw;
          }
          LOG(WARNING)
              << "CudfHashJoinProbe: probe coalescing failed for planNode "
              << joinNode_->id() << " (" << accumulatedProbeInputs_.size()
              << " batches, " << accumulatedProbeRows_ << " rows): "
              << e.what() << ". Processing first batch only.";
          recoverGpuMemory();
          input_ = std::move(accumulatedProbeInputs_[0]);
        }
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
    if ((joinNode_->isRightJoin() || joinNode_->isFullJoin()) && noMoreInput_ &&
        !finished_ && isLastDriver_) {
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
        // Build a boolean mask: unmatched = NOT(flags)
        auto boolMask = cudf::unary_operation(
            flags->view(), cudf::unary_operator::NOT, stream);

        // Count unmatched rows by summing the boolean mask
        auto unmatchedCountScalar = cudf::reduce(
            boolMask->view(),
            *cudf::make_sum_aggregation<cudf::reduce_aggregation>(),
            cudf::data_type{cudf::type_id::INT32},
            stream);
        auto m = static_cast<cudf::numeric_scalar<int32_t>*>(
                     unmatchedCountScalar.get())
                     ->value(stream);
        if (m == 0) {
          continue;
        }

        // Build left null columns
        std::vector<std::unique_ptr<cudf::column>> outCols(outputType_->size());
        // Left side nulls (types derive from probe schema at the matching
        // channel indices)
        for (size_t li = 0; li < leftColumnOutputIndices_.size(); ++li) {
          auto outIdx = leftColumnOutputIndices_[li];
          auto probeChannel = leftColumnIndicesToGather_[li];
          auto leftCudfDataType =
              veloxToCudfDataType(probeType_->childAt(probeChannel));
          auto nullScalar = cudf::make_default_constructed_scalar(
              leftCudfDataType, stream, cudf::get_current_device_resource_ref());
          outCols[outIdx] = cudf::make_column_from_scalar(
              *nullScalar, m, stream, cudf::get_current_device_resource_ref());
        }
        // Right side - gather unmatched build columns if any
        if (!rightColumnIndicesToGather_.empty()) {
          auto rightInput =
              rightTable->view().select(rightColumnIndicesToGather_);
          auto unmatchedRight =
              cudf::apply_boolean_mask(rightInput, boolMask->view(), stream);
          auto rightCols = unmatchedRight->release();
          for (size_t ri = 0; ri < rightColumnOutputIndices_.size(); ++ri) {
            auto outIdx = rightColumnOutputIndices_[ri];
            outCols[outIdx] = std::move(rightCols[ri]);
          }
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

  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input_);
  VELOX_CHECK_NOT_NULL(cudfInput);
  auto stream = cudfInput->stream();

  // Detect sticky CUDA errors from prior operations before starting the join.
  // Proceeding with a corrupted context will crash in RMM deallocate_async.
  checkCudaHealth("CudfHashJoinProbe::getOutput entry");

  // Use getTableView() to avoid expensive materialization for packed_table.
  // cudfInput is staying alive until the table view is no longer needed.
  auto leftTableView = cudfInput->getTableView();
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "Probe table number of columns: " << leftTableView.num_columns();
    VLOG(1) << "Probe table number of rows: " << leftTableView.num_rows();
  }

  if (leftTableView.num_rows() == 0) {
    cudfInput.reset();
    input_.reset();
    finished_ =
        noMoreInput_ && !joinNode_->isRightJoin() && !joinNode_->isFullJoin();
    return nullptr;
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

  {
    auto n = leftTableView.num_rows();
    if (n > 0) {
      try {
        auto nullKeyRows = countNullKeyRows(
            leftTableView, leftKeyIndices_, stream);
        if (nullKeyRows > 0) {
          auto lockedStats = stats_.wlock();
          lockedStats->numNullKeys += nullKeyRows;
        }
      } catch (const std::exception& e) {
        if (!isCudaRelatedError(e)) {
          throw;
        }
        LOG(WARNING)
            << "CudfHashJoinProbe: null key counting failed for planNode "
            << joinNode_->id() << " (probe=" << n << " rows): " << e.what()
            << ". Skipping null key stats.";
        cudaGetLastError();
      }
    }
  }

  // Build hash tables on-demand (transient). Construction is deferred from
  // the build phase to here so that at most GpuGuard-max hash table sets
  // exist at any instant, rather than one per Spark task.
  bool const needHashJoin =
      joinNode_->isInnerJoin() || joinNode_->isLeftJoin() ||
      joinNode_->isRightJoin() || joinNode_->isFullJoin();

  // Estimate build-side memory for large-join detection.
  size_t buildBytesTotal = 0;
  {
    auto& rt = hashObject_.value().first;
    for (const auto& t : rt) {
      buildBytesTotal +=
          static_cast<size_t>(t->num_rows()) * t->num_columns() * 16;
    }
  }

  // Acquire exclusive large-join lock if this join is memory-heavy.
  // This serializes large hash joins that would otherwise corrupt the
  // RMM pool when running concurrently under maxConcurrentGpuTasks>1.
  std::unique_lock<std::mutex> largeJoinLock;
  {
    size_t freeMem = 0, totalMem = 0;
    if (cudaMemGetInfo(&freeMem, &totalMem) == cudaSuccess && totalMem > 0) {
      size_t joinFootprint = buildBytesTotal * 3;
      if (joinFootprint >
          static_cast<size_t>(totalMem * kLargeJoinMemoryFraction)) {
        LOG(INFO) << "Large join detected for planNode " << joinNode_->id()
                  << " (buildEstMB=" << (buildBytesTotal >> 20)
                  << ", footprintMB=" << (joinFootprint >> 20)
                  << ", totalGpuMB=" << (totalMem >> 20)
                  << "). Serializing with other large joins.";
        largeJoinLock = std::unique_lock<std::mutex>(sLargeJoinMutex);
        recoverGpuMemory();
      }
    }
  }

  std::vector<std::unique_ptr<cudf::table>> cudfOutputs;
  bool usedPartitionedJoin = false;

  try {

  // --- Grace (partitioned) hash join ---
  // When the hash table exceeds available GPU memory, partition both build
  // and probe by join key hash and process each partition independently.
  // Peak memory: ~2B + P + 3B/N  (vs ~3B + P for non-partitioned).
  {
    bool const canPartition = needHashJoin &&
        (joinNode_->isInnerJoin() || joinNode_->isLeftJoin());

    if (canPartition) {
      size_t gpuFree = 0, gpuTotal = 0;
      if (cudaMemGetInfo(&gpuFree, &gpuTotal) == cudaSuccess && gpuFree > 0) {
        size_t hashTableEst = buildBytesTotal * 2;
        if (hashTableEst > gpuFree * 60 / 100) {
          // Size partitions so each hash table fits in ~25% of free memory.
          int numPartitions = std::max(
              2,
              static_cast<int>(
                  (4 * hashTableEst + gpuFree - 1) / gpuFree));
          numPartitions = std::min(numPartitions, 32);

          LOG(INFO)
              << "Grace hash join for planNode " << joinNode_->id()
              << ": " << numPartitions << " partitions"
              << " (buildEstMB=" << (buildBytesTotal >> 20)
              << ", htEstMB=" << (hashTableEst >> 20)
              << ", freeMB=" << (gpuFree >> 20)
              << ", probeRows=" << leftTableView.num_rows() << ")";

          // Concatenate build tables if split across multiple chunks.
          auto& origRT = hashObject_.value().first;
          cudf::table_view buildView = origRT[0]->view();
          std::unique_ptr<cudf::table> concatBuild;
          if (origRT.size() > 1) {
            std::vector<cudf::table_view> views;
            for (const auto& t : origRT) views.push_back(t->view());
            concatBuild = cudf::concatenate(views, stream);
            buildView = concatBuild->view();
          }

          // Partition both sides with identical hash function + seed so
          // matching rows land in the same bucket.
          auto [partBuild, buildOffsets] = cudf::hash_partition(
              buildView,
              rightKeyIndices_,
              numPartitions,
              cudf::hash_id::HASH_MURMUR3,
              0,
              stream);

          auto [partProbe, probeOffsets] = cudf::hash_partition(
              leftTableView,
              leftKeyIndices_,
              numPartitions,
              cudf::hash_id::HASH_MURMUR3,
              0,
              stream);

          // Release build concat only; keep cudfInput/input_ alive so we
          // can fall back to the regular probe-split path if any partition
          // fails with a CUDA error.
          concatBuild.reset();
          recoverGpuMemory();

          // offsets vector has num_partitions+1 elements.
          std::vector<cudf::size_type> bSplits(
              buildOffsets.begin() + 1, buildOffsets.end() - 1);
          auto buildParts = cudf::split(partBuild->view(), bSplits);

          std::vector<cudf::size_type> pSplits(
              probeOffsets.begin() + 1, probeOffsets.end() - 1);
          auto probeParts = cudf::split(partProbe->view(), pSplits);

          // Save class state that join methods read; restore after loop.
          auto savedHash = std::move(hashObject_);
          bool savedAst = useAstFilter_;
          auto savedRP = std::move(cachedRightPrecomputed_);
          auto savedEV = std::move(cachedExtendedRightViews_);
          auto savedBS = buildStream_;

          useAstFilter_ = false;
          cachedRightPrecomputed_.clear();
          cachedExtendedRightViews_.clear();
          buildStream_ = std::nullopt;

          auto restoreState = [&]() {
            hashObject_ = std::move(savedHash);
            useAstFilter_ = savedAst;
            cachedRightPrecomputed_ = std::move(savedRP);
            cachedExtendedRightViews_ = std::move(savedEV);
            buildStream_ = savedBS;
          };

          bool graceJoinSucceeded = true;
          try {
            for (int p = 0; p < numPartitions; ++p) {
              auto bPart = buildParts[p];
              auto pPart = probeParts[p];

              if (pPart.num_rows() == 0) continue;
              if (bPart.num_rows() == 0 && joinNode_->isInnerJoin()) continue;

              auto partTable = std::make_shared<cudf::table>(bPart);
              auto partHJ = std::make_shared<cudf::hash_join>(
                  partTable->view().select(rightKeyIndices_),
                  cudf::null_equality::UNEQUAL,
                  stream);

              std::vector<std::shared_ptr<cudf::table>> pt = {partTable};
              std::vector<std::shared_ptr<cudf::hash_join>> ph = {partHJ};
              hashObject_ = std::make_optional(
                  std::make_pair(std::move(pt), std::move(ph)));

              auto partJoinFn = [&](cudf::table_view pv) {
                return joinNode_->isInnerJoin()
                    ? innerJoin(pv, stream)
                    : leftJoin(pv, stream);
              };
              auto noFallback = [](cudf::table_view, rmm::cuda_stream_view)
                  -> std::vector<std::unique_ptr<cudf::table>> {
                return {};
              };
              auto partResults = joinWithAdaptiveSplit(
                  pPart, stream, partJoinFn,
                  fmt::format("Grace partition {} planNode {}",
                              p, joinNode_->id()),
                  noFallback);
              for (auto& r : partResults) {
                cudfOutputs.push_back(std::move(r));
              }

              // Release partition hash table before next partition.
              hashObject_.reset();
              recoverGpuMemory();
            }
          } catch (const std::exception& graceErr) {
            restoreState();
            if (!isCudaRelatedError(graceErr)) {
              throw;
            }
            LOG(WARNING)
                << "Grace hash join failed for planNode " << joinNode_->id()
                << ": " << graceErr.what()
                << ". Discarding partial results and falling back to "
                << "regular probe-split path.";
            recoverGpuMemory();
            cudfOutputs.clear();
            graceJoinSucceeded = false;
          }

          if (graceJoinSucceeded) {
            restoreState();
            cudfInput.reset();
            input_.reset();
            usedPartitionedJoin = true;
          }
        }
      }
    }
  }

  if (!usedPartitionedJoin) {

  if (needHashJoin) {
    auto& rightTables = hashObject_.value().first;
    auto& hbs = hashObject_.value().second;
    for (size_t i = 0; i < rightTables.size(); ++i) {
      if (!hbs[i]) {
        ensureGpuMemoryAvailable(
            128ULL << 20, "CudfHashJoinProbe hash table construction");
        for (int attempt = 0;; ++attempt) {
          try {
            hbs[i] = std::make_shared<cudf::hash_join>(
                rightTables[i]->view().select(rightKeyIndices_),
                cudf::null_equality::UNEQUAL,
                stream);
            break;
          } catch (const std::exception& e) {
            if (!isCudaRelatedError(e)) {
              throw;
            }
            {
              auto err = cudaPeekAtLastError();
              if (err != cudaSuccess && err != cudaErrorMemoryAllocation &&
                  err != cudaErrorInvalidValue) {
                VELOX_FAIL(
                    "CUDA device error {} ({}) building hash table for "
                    "planNode {} batch {}. Aborting: {}",
                    static_cast<int>(err),
                    cudaGetErrorString(err),
                    joinNode_->id(),
                    i,
                    e.what());
              }
              if (err == cudaErrorInvalidValue) {
                cudaGetLastError();
              }
            }
            if (isFatalCudaError(e)) {
              VELOX_FAIL(
                  "Fatal CUDA error building hash table for planNode {} "
                  "batch {} (detected from exception message). Aborting: {}",
                  joinNode_->id(),
                  i,
                  e.what());
            }
            if (attempt >= kOomMaxRetries) {
              throw;
            }
            LOG(WARNING)
                << "CudfHashJoinProbe OOM building hash table for planNode "
                << joinNode_->id() << " batch " << i << " (attempt "
                << (attempt + 1) << "/" << kOomMaxRetries << "): " << e.what()
                << ". Recovering GPU memory and retrying.";
            recoverGpuMemory();
            std::this_thread::sleep_for(
                std::chrono::milliseconds(200 * (1 << attempt)));
          }
        }
      }
    }
  }

  // Validate join key types between probe and build. Log mismatches for
  // diagnostics — these are handled by alignProbeKeyTypes in each join
  // method, but logging helps trace CUDA errors to specific type issues.
  if (CudfConfig::getInstance().debugEnabled) {
    auto& rightTables = hashObject_.value().first;
    if (!rightTables.empty() && rightTables[0]->num_rows() > 0) {
      auto buildView = rightTables[0]->view();
      for (size_t k = 0; k < leftKeyIndices_.size(); ++k) {
        auto pType = leftTableView.column(leftKeyIndices_[k]).type();
        auto bType = buildView.column(rightKeyIndices_[k]).type();
        if (pType != bType) {
          VLOG(1) << "Key type mismatch at index " << k
                  << " for planNode " << joinNode_->id()
                  << ": probe=" << static_cast<int>(pType.id())
                  << " (scale=" << pType.scale() << ")"
                  << " build=" << static_cast<int>(bType.id())
                  << " (scale=" << bType.scale() << ")";
        }
      }
    }
  }

  auto executeJoin = [&](cudf::table_view probeView)
      -> std::vector<std::unique_ptr<cudf::table>> {
    switch (joinNode_->joinType()) {
      case core::JoinType::kInner:
        return innerJoin(probeView, stream);
      case core::JoinType::kLeft:
        return leftJoin(probeView, stream);
      case core::JoinType::kRight:
        return rightJoin(probeView, stream);
      case core::JoinType::kLeftSemiFilter:
        return leftSemiFilterJoin(probeView, stream);
      case core::JoinType::kLeftSemiProject:
        return leftSemiProjectJoin(probeView, stream);
      case core::JoinType::kRightSemiFilter:
        return rightSemiFilterJoin(probeView, stream);
      case core::JoinType::kAnti:
        return antiJoin(probeView, stream);
      case core::JoinType::kFull:
        return fullJoin(probeView, stream);
      default:
        VELOX_FAIL("Unsupported join type: ", joinNode_->joinType());
    }
  };

  bool const canSplitProbe =
      joinNode_->isInnerJoin() || joinNode_->isLeftJoin() ||
      joinNode_->isLeftSemiFilterJoin() ||
      joinNode_->isLeftSemiProjectJoin() || joinNode_->isAntiJoin() ||
      joinNode_->isRightJoin() || joinNode_->isFullJoin();
  static constexpr cudf::size_type kMinSplitRows = 1;

  std::vector<cudf::table_view> probeSlices;

  // Phase 1: Exact size-based proactive probe splitting.
  // Uses cudf::hash_join::*_join_size() on the FULL probe to compute the
  // precise output row count before any allocation. This prevents
  // cudaErrorInvalidValue from oversized gather map allocations.
  if (needHashJoin && canSplitProbe &&
      leftTableView.num_rows() > kMinSplitRows) {
    auto& sizeRT = hashObject_.value().first;
    auto& sizeHbs = hashObject_.value().second;

    size_t totalEstOutput = 0;
    bool sizeOK = false;
    try {
      for (size_t bi = 0; bi < sizeRT.size(); ++bi) {
        if (!sizeHbs[bi] || sizeRT[bi]->num_rows() == 0) {
          continue;
        }
        std::vector<std::unique_ptr<cudf::column>> tmpCasts;
        auto aligned = alignProbeKeyTypes(
            leftTableView, leftKeyIndices_,
            sizeRT[bi]->view(), rightKeyIndices_,
            tmpCasts, stream);
        auto probeKeys = aligned.select(leftKeyIndices_);

        if (joinNode_->isInnerJoin() || joinNode_->isRightJoin()) {
          totalEstOutput +=
              sizeHbs[bi]->inner_join_size(probeKeys, stream);
        } else if (joinNode_->isLeftJoin() || joinNode_->isFullJoin()) {
          totalEstOutput +=
              sizeHbs[bi]->left_join_size(probeKeys, stream);
        }
      }
      sizeOK = true;
    } catch (const std::exception& sizeErr) {
      LOG(WARNING) << "Join size estimation failed for planNode "
                   << joinNode_->id() << ": " << sizeErr.what()
                   << ". Falling back to heuristic.";
      recoverGpuMemory();
    }

    if (sizeOK && totalEstOutput > 0) {
      auto maxSafe = computeMaxSafeJoinOutputRows(stream);
      if (totalEstOutput > maxSafe) {
        // Add 50% extra chunks to handle key-value skew.
        auto numChunks = static_cast<cudf::size_type>(std::max(
            size_t(2),
            (totalEstOutput + maxSafe - 1) / maxSafe * 3 / 2));
        auto rowsPerChunk = std::max(
            cudf::size_type(1),
            leftTableView.num_rows() / numChunks);

        LOG(INFO)
            << "Exact-size probe split for planNode " << joinNode_->id()
            << ": estOutput=" << totalEstOutput << ", maxSafe=" << maxSafe
            << ", probeRows=" << leftTableView.num_rows()
            << ", chunks=" << numChunks
            << ", rowsPerChunk=" << rowsPerChunk;

        std::vector<cudf::size_type> splitPts;
        for (cudf::size_type si = rowsPerChunk;
             si < leftTableView.num_rows();
             si += rowsPerChunk) {
          splitPts.push_back(si);
        }
        auto chunks = cudf::split(leftTableView, splitPts, stream);
        for (int j = static_cast<int>(chunks.size()) - 1; j >= 0; --j) {
          probeSlices.push_back(chunks[j]);
        }
      }
    }
  }

  // Phase 2: Heuristic-based fallback when exact estimation is unavailable
  // (non-hash joins, estimation failed, etc.).
  if (probeSlices.empty() && canSplitProbe &&
      leftTableView.num_rows() > kMinSplitRows) {
    size_t freeMem = 0, totalMem = 0;
    if (cudaMemGetInfo(&freeMem, &totalMem) == cudaSuccess && totalMem > 0) {
      size_t outputRowBytes =
          std::max(size_t(16), static_cast<size_t>(outputType_->size()) * 8);

      auto& rightTables = hashObject_.value().first;
      size_t totalBuildRows = 0;
      size_t buildBytesEstimate = 0;
      for (const auto& rt : rightTables) {
        totalBuildRows += rt->num_rows();
        buildBytesEstimate +=
            static_cast<size_t>(rt->num_rows()) * rt->num_columns() * 16;
      }
      size_t amplification = std::min(
          size_t(1000000),
          std::max(size_t(1000), totalBuildRows / 50));
      size_t costPerProbeRow = (8 + outputRowBytes) * amplification;

      size_t hashTableFootprint = buildBytesEstimate * 2;
      size_t usableFree =
          (freeMem > hashTableFootprint)
          ? (freeMem - hashTableFootprint)
          : (freeMem / 4);
      size_t maxAlloc = usableFree / 100;
      auto maxRows = static_cast<cudf::size_type>(std::min(
          static_cast<size_t>(leftTableView.num_rows()),
          std::max(
              static_cast<size_t>(kMinSplitRows),
              maxAlloc / std::max(costPerProbeRow, size_t(1)))));
      if (maxRows < leftTableView.num_rows()) {
        LOG(INFO)
            << "Heuristic probe split for planNode " << joinNode_->id()
            << ": " << leftTableView.num_rows() << " rows -> chunks of "
            << maxRows << " (freeMem=" << (freeMem >> 20) << "MB"
            << ", totalMem=" << (totalMem >> 20) << "MB"
            << ", buildRows=" << totalBuildRows
            << ", amplification=" << amplification << "x)";
        std::vector<cudf::size_type> splitIndices;
        for (cudf::size_type i = maxRows; i < leftTableView.num_rows();
             i += maxRows) {
          splitIndices.push_back(i);
        }
        auto chunks = cudf::split(leftTableView, splitIndices, stream);
        for (int j = static_cast<int>(chunks.size()) - 1; j >= 0; --j) {
          probeSlices.push_back(chunks[j]);
        }
      }
    }
  }
  if (probeSlices.empty()) {
    probeSlices.push_back(leftTableView);
  }

  // Process probe slices with recursive adaptive splitting. On CUDA error
  // or size_type overflow, joinWithAdaptiveSplit bisects the probe and retries
  // each half -- errors are caught and handled INSIDE the operator before they
  // reach Velox's error framework (which marks them as non-retriable).
  auto joinContext = fmt::format(
      "CudfHashJoinProbe planNode {}", joinNode_->id());

  for (int si = static_cast<int>(probeSlices.size()) - 1; si >= 0; --si) {
    auto slice = probeSlices[si];
    if (slice.num_rows() == 0) continue;

    // Pre-join memory check: reclaim deferred frees when memory is tight.
    {
      size_t freeMem = 0, totalMem = 0;
      if (cudaMemGetInfo(&freeMem, &totalMem) == cudaSuccess &&
          totalMem > 0 && freeMem < totalMem / 8) {
        recoverGpuMemory();
      }
    }

    if (canSplitProbe) {
      auto buildChunkFallback =
          [this](cudf::table_view pv, rmm::cuda_stream_view s)
              -> std::vector<std::unique_ptr<cudf::table>> {
        return joinWithChunkedBuild(pv, s);
      };
      auto results = joinWithAdaptiveSplit(
          slice, stream, executeJoin, joinContext, buildChunkFallback);
      for (auto& r : results) {
        cudfOutputs.push_back(std::move(r));
      }
    } else {
      // Join types that can't be split (e.g. rightSemiFilter): retry only.
      for (int attempt = 0;; ++attempt) {
        try {
          auto results = executeJoin(slice);
          for (auto& r : results) {
            cudfOutputs.push_back(std::move(r));
          }
          break;
        } catch (const std::exception& e) {
          if (!isCudaRelatedError(e) || isFatalCudaError(e) ||
              attempt >= kOomMaxRetries) {
            throw;
          }
          cudaGetLastError();
          recoverGpuMemory();
          LOG(WARNING)
              << joinContext << ": retry " << (attempt + 1)
              << "/" << kOomMaxRetries
              << " for unsplittable join: " << e.what();
          std::this_thread::sleep_for(
              std::chrono::milliseconds(200 * (1 << attempt)));
        }
      }
    }
  }

  } // !usedPartitionedJoin

  } catch (const std::exception& outerEx) {
    // Catch-all for any CUDA error that escaped the retry/split mechanism.
    // This prevents the Velox operator framework from seeing the raw
    // rmm::cuda_error and marking the task as non-retriable.
    if (isCudaRelatedError(outerEx) && !isFatalCudaError(outerEx)) {
      LOG(ERROR) << "Unhandled CUDA error in getOutput for planNode "
                 << joinNode_->id() << " (joinType="
                 << static_cast<int>(joinNode_->joinType())
                 << "): " << outerEx.what()
                 << ". Recovering and skipping this probe batch.";
      recoverGpuMemory();
      cudfInput.reset();
      input_.reset();
      finished_ = noMoreInput_ && !joinNode_->isRightJoin() &&
          !joinNode_->isFullJoin();
      return nullptr;
    }
    throw;
  }

  // Synchronize the probe stream and build stream before releasing any GPU
  // objects. Without this, hash_join / gather kernels still in flight on the
  // build stream can reference memory that is freed below, corrupting the
  // RMM async pool metadata (manifests as SIGSEGV in deallocate_async).
  try {
    stream.synchronize();
    if (buildStream_.has_value()) {
      buildStream_.value().synchronize();
    }
    checkCudaHealth("CudfHashJoinProbe before hash table release");
  } catch (const std::exception& syncEx) {
    if (isCudaRelatedError(syncEx) && !isFatalCudaError(syncEx)) {
      LOG(ERROR) << "CUDA error during post-join sync for planNode "
                 << joinNode_->id() << ": " << syncEx.what()
                 << ". Clearing error and continuing with available results.";
      cudaDeviceSynchronize();
      cudaGetLastError();
    } else {
      throw;
    }
  }

  // Release the large-join lock now that all GPU-heavy work is done.
  // Hash table/input release below is lightweight (just refcount + free);
  // letting other large joins start sooner improves overall throughput.
  if (largeJoinLock.owns_lock()) {
    largeJoinLock.unlock();
  }

  // Release transient hash tables immediately after probing to free GPU
  // memory for other tasks. They'll be rebuilt on the next getOutput() call.
  if (needHashJoin) {
    auto& hbs = hashObject_.value().second;
    for (auto& hb : hbs) {
      hb.reset();
    }
  }

  // Release input CudfVector to free GPU memory before creating output.
  // This reduces peak memory from (input + output) to max(input, output).
  // cudfInput must be released first since input_.reset() only decrements
  // the refcount while cudfInput still holds a reference.
  cudfInput.reset();
  input_.reset();

  // Force deferred frees to complete so that memory from the released hash
  // tables and input is actually available for other tasks/allocations.
  // recoverGpuMemory() will throw if the device is fatally corrupted.
  recoverGpuMemory();

  // Remove empty tables before deciding how to return.
  cudfOutputs.erase(
      std::remove_if(
          cudfOutputs.begin(),
          cudfOutputs.end(),
          [](const std::unique_ptr<cudf::table>& t) {
            return !t || t->num_rows() == 0 || t->num_columns() == 0;
          }),
      cudfOutputs.end());

  if (cudfOutputs.empty()) {
    finished_ =
        noMoreInput_ && !joinNode_->isRightJoin() && !joinNode_->isFullJoin();
    return nullptr;
  }

  if (cudfOutputs.size() == 1) {
    // Single result — return directly without concatenation.
    finished_ =
        noMoreInput_ && !joinNode_->isRightJoin() && !joinNode_->isFullJoin();
    auto tbl = std::move(cudfOutputs[0]);
    return std::make_shared<CudfVector>(
        pool(), outputType_, tbl->num_rows(), std::move(tbl), stream);
  }

  // Multiple split results (from OOM probe splitting).
  // Instead of concatenating (which doubles peak GPU memory), store them
  // and return one per getOutput() call — analogous to Spark RAPIDS
  // JoinGatherer batched output pattern.
  pendingJoinOutputs_ = std::move(cudfOutputs);
  // Reverse so that pop_back returns results in order.
  std::reverse(pendingJoinOutputs_.begin(), pendingJoinOutputs_.end());

  auto tbl = std::move(pendingJoinOutputs_.back());
  pendingJoinOutputs_.pop_back();
  if (pendingJoinOutputs_.empty()) {
    finished_ =
        noMoreInput_ && !joinNode_->isRightJoin() && !joinNode_->isFullJoin();
  }
  return std::make_shared<CudfVector>(
      pool(), outputType_, tbl->num_rows(), std::move(tbl), stream);
}

bool CudfHashJoinProbe::skipProbeOnEmptyBuild() const {
  auto const joinType = joinNode_->joinType();
  return isInnerJoin(joinType) || isLeftSemiFilterJoin(joinType) ||
      isRightJoin(joinType) || isRightSemiFilterJoin(joinType) ||
      isRightSemiProjectJoin(joinType);
}

exec::BlockingReason CudfHashJoinProbe::isBlocked(ContinueFuture* future) {
  if ((joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin() ||
       joinNode_->isFullJoin()) &&
      hashObject_.has_value()) {
    if (!future_.valid()) {
      return exec::BlockingReason::kNotBlocked;
    }
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForJoinProbe;
  }

  if (hashObject_.has_value()) {
    return exec::BlockingReason::kNotBlocked;
  }

  const auto splitGroupId = operatorCtx_->driverCtx()->splitGroupId;
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      splitGroupId, planNodeId());
  auto cudfJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  if (CudfConfig::getInstance().debugEnabled) {
    VLOG(1) << "CudfHashJoinProbe bridge lookup: planNodeId=" << planNodeId()
            << ", splitGroupId=" << splitGroupId
            << ", joinBridge=" << static_cast<const void*>(joinBridge.get())
            << ", cudfJoinBridge="
            << static_cast<const void*>(cudfJoinBridge.get());
  }
  VELOX_CHECK_NOT_NULL(
      joinBridge,
      "Expected JoinBridge for CudfHashJoinProbe. planNodeId: {}, splitGroupId: {}",
      planNodeId(),
      splitGroupId);
  VELOX_CHECK_NOT_NULL(
      cudfJoinBridge,
      "Expected CudfHashJoinBridge for CudfHashJoinProbe. planNodeId: {}, splitGroupId: {}, joinBridge: {}",
      planNodeId(),
      splitGroupId,
      static_cast<const void*>(joinBridge.get()));
  VELOX_CHECK_NOT_NULL(future);
  auto hashObject = cudfJoinBridge->hashOrFuture(future);

  if (!hashObject.has_value()) {
    if (CudfConfig::getInstance().debugEnabled) {
      VLOG(2) << "CudfHashJoinProbe is blocked, waiting for join build";
    }
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  hashObject_ = std::move(hashObject);
  buildStream_ = cudfJoinBridge->getBuildStream();

  // Lazy initialize matched flags only when build side is done
  if (joinNode_->isRightJoin() || joinNode_->isFullJoin()) {
    auto& rightTablesInit = hashObject_.value().first;
    rightMatchedFlags_.clear();
    rightMatchedFlags_.reserve(rightTablesInit.size());
    auto initStream = cudfGlobalStreamPool().get_stream();
    for (auto& rt : rightTablesInit) {
      auto n = rt->num_rows();
      if (n == 0) {
        rightMatchedFlags_.push_back(
            cudf::make_empty_column(cudf::data_type(cudf::type_id::BOOL8)));
        continue;
      }
      auto false_scalar = cudf::numeric_scalar<bool>(false, true, initStream);
      auto flags_col = cudf::make_column_from_scalar(
          false_scalar, n, initStream, cudf::get_current_device_resource_ref());
      rightMatchedFlags_.push_back(std::move(flags_col));
    }
    initStream.synchronize();
  }

  // Precompute right table columns if filter exists (once when build is done)
  if (joinNode_->filter() && useAstFilter_ &&
      !rightPrecomputeInstructions_.empty()) {
    try {
      auto& rightTablesInit = hashObject_.value().first;
      cachedRightPrecomputed_.clear();
      cachedExtendedRightViews_.clear();
      cachedRightPrecomputed_.reserve(rightTablesInit.size());
      cachedExtendedRightViews_.reserve(rightTablesInit.size());

      auto initStream = cudfGlobalStreamPool().get_stream();
      for (auto& rt : rightTablesInit) {
        auto rightTableView = rt->view();
        if (rightTableView.num_rows() == 0) {
          cachedRightPrecomputed_.emplace_back();
          cachedExtendedRightViews_.push_back(rightTableView);
          continue;
        }
        auto rightColumnViews = tableViewToColumnViews(rightTableView);
        auto rightPrecomputed = precomputeSubexpressions(
            rightColumnViews,
            rightPrecomputeInstructions_,
            scalars_,
            buildType_,
            initStream);
        auto extendedView =
            createExtendedTableView(rightTableView, rightPrecomputed);
        cachedRightPrecomputed_.push_back(std::move(rightPrecomputed));
        cachedExtendedRightViews_.push_back(extendedView);
      }
      initStream.synchronize();
    } catch (const VeloxException& e) {
      LOG(WARNING)
          << "CudfHashJoinProbe: right-side precompute failed, "
          << "disabling AST filter for planNode " << joinNode_->id()
          << ": " << e.what();
      useAstFilter_ = false;
      cachedRightPrecomputed_.clear();
      cachedExtendedRightViews_.clear();
    }
  }

  auto& rightTables = hashObject_.value().first;
  if (!rightTables.empty() && rightTables[0]->num_rows() == 0) {
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
  if ((joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin() ||
       joinNode_->isFullJoin()) &&
      future_.valid()) {
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForJoinProbe;
  }
  return exec::BlockingReason::kNotBlocked;
}

bool CudfHashJoinProbe::isFinished() {
  bool isFinished;
  if ((joinNode_->isRightJoin() || joinNode_->isFullJoin()) &&
      isLastDriver_) {
    // The last driver must wait until finished_ is set after emitting
    // unmatched build rows.
    isFinished = finished_;
  } else {
    isFinished = finished_ ||
        (noMoreInput_ && input_ == nullptr &&
         accumulatedProbeInputs_.empty() &&
         pendingJoinOutputs_.empty());
  }

  if (isFinished) {
    hashObject_.reset();
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
