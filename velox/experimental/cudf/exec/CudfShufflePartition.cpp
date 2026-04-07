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

#include "velox/experimental/cudf/exec/CudfShufflePartition.h"

#include <cudf/binaryop.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cuda_runtime.h>

namespace facebook::velox::cudf_velox {

CudfShufflePartition::CudfShufflePartition(
    int32_t operatorId,
    RowTypePtr outputType,
    exec::DriverCtx* driverCtx,
    std::string planNodeId,
    int32_t numPartitions)
    : exec::Operator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "CudfShufflePartition"),
      NvtxHelper(
          nvtx3::rgb{0, 191, 255}, // Deep sky blue
          operatorId,
          fmt::format("[{}]", planNodeId)),
      numPartitions_(numPartitions) {
  VELOX_CHECK_GT(numPartitions_, 0, "numPartitions must be > 0");
}

void CudfShufflePartition::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  VELOX_CHECK_NULL(output_, "Previous output not consumed");
  VELOX_CHECK(
      !streamPending_.load(std::memory_order_acquire),
      "Stream operation already in-flight");

  auto cudfVec = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfVec, "Input must be a CudfVector");

  auto tableView = cudfVec->getTableView();
  auto stream = cudfVec->stream();
  VELOX_CHECK_GT(
      tableView.num_columns(),
      0,
      "Input table must have at least one column (hash)");

  // First column is the hash value produced by the exchange hash expression.
  auto firstCol = tableView.column(0);

  // PID = hash % numPartitions — enqueued to |stream|, runs asynchronously.
  auto numPartScalar = cudf::numeric_scalar<int32_t>(numPartitions_, true, stream);
  auto pidCol = cudf::binary_operation(
      firstCol,
      numPartScalar,
      cudf::binary_operator::PYMOD,
      cudf::data_type{cudf::type_id::INT32},
      stream);

  // Build table with PID replacing hash as column 0.
  std::vector<cudf::column_view> cols;
  cols.reserve(tableView.num_columns());
  cols.push_back(pidCol->view());
  for (cudf::size_type i = 1; i < tableView.num_columns(); ++i) {
    cols.push_back(tableView.column(i));
  }
  cudf::table_view fullTable(cols);

  // cudf::partition reorders rows so that rows with the same PID are
  // contiguous.  offsets[i] is the start index of partition i.
  // Both binary_operation and partition are enqueued to |stream|; the GPU
  // executes them asynchronously while the CPU continues below.
  auto [partitioned, offsets] = cudf::partition(
      fullTable,
      pidCol->view(),
      static_cast<cudf::size_type>(numPartitions_),
      stream);
  VELOX_CHECK_EQ(
      offsets.size(),
      static_cast<size_t>(numPartitions_) + 1,
      "cudf::partition must return numPartitions+1 offsets");

  // Wrap the partitioned GPU table immediately.  The CudfVector object is
  // CPU-side; the underlying GPU memory is valid once the stream operations
  // above complete.  We store it now and only expose it to the caller after
  // the stream callback confirms completion.
  output_ = std::make_shared<CudfVector>(
      pool(),
      outputType_,
      partitioned->num_rows(),
      std::move(partitioned),
      stream);

  // Create a promise/future pair to wake the Velox driver when the GPU work
  // completes.  The future is consumed by isBlocked(); the promise is moved
  // into the callback context and fulfilled by the CUDA host function.
  auto [promise, future] =
      makeVeloxContinuePromiseContract("CudfShufflePartition::stream");
  streamFuture_ = std::move(future);

  // Heap-allocate context to outlive this stack frame.  Ownership transfers
  // to the CUDA host function; it deletes ctx after setValue().
  struct CallbackCtx {
    CudfShufflePartition* self;
    ContinuePromise promise;
  };
  auto* ctx = new CallbackCtx{this, std::move(promise)};

  // Mark in-flight BEFORE registering the callback to avoid a race where the
  // callback fires before streamPending_ is set.
  streamPending_.store(true, std::memory_order_release);

  // cudaLaunchHostFunc enqueues a host-side callback at the tail of |stream|.
  // It fires on a CUDA internal thread once all preceding stream work is done,
  // at which point the partitioned GPU table is fully materialized.
  const cudaError_t err = cudaLaunchHostFunc(
      stream.value(),
      [](void* userData) {
        auto* c = static_cast<CallbackCtx*>(userData);
        // Release the pending flag so the Velox driver sees the work is done.
        c->self->streamPending_.store(false, std::memory_order_release);
        // Wake the driver thread that is parked on streamFuture_.
        c->promise.setValue();
        delete c;
      },
      ctx);

  if (err != cudaSuccess) {
    // Roll back: undo the pending flag and free context so the operator stays
    // in a consistent state and the error propagates cleanly.
    streamPending_.store(false, std::memory_order_release);
    delete ctx;
    streamFuture_.reset();
    VELOX_FAIL(
        "CudfShufflePartition: cudaLaunchHostFunc failed: {}",
        cudaGetErrorString(err));
  }
}

RowVectorPtr CudfShufflePartition::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (!output_) {
    finished_ = noMoreInput_;
    return nullptr;
  }
  // GPU kernels are still in-flight; isBlocked() parks the driver until the
  // CUDA host callback fires and clears streamPending_.
  if (streamPending_.load(std::memory_order_acquire)) {
    return nullptr;
  }
  // Stream work is complete; GPU table is fully materialized.
  auto result = std::move(output_);
  finished_ = noMoreInput_;
  return result;
}

exec::BlockingReason CudfShufflePartition::isBlocked(ContinueFuture* future) {
  if (streamPending_.load(std::memory_order_acquire)) {
    if (streamFuture_.has_value()) {
      *future = std::move(*streamFuture_);
      streamFuture_.reset();
    }
    return exec::BlockingReason::kWaitForStream;
  }
  return exec::BlockingReason::kNotBlocked;
}

} // namespace facebook::velox::cudf_velox
