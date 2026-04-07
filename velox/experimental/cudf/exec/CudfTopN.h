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
#pragma once

#include "velox/common/future/VeloxPromise.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/exec/Operator.h"

namespace facebook::velox::cudf_velox {

class CudfTopN : public exec::Operator, public NvtxHelper {
 public:
  CudfTopN(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::TopNNode>& topNNode);

  // Returns false while a CUDA stream is in flight so the Velox driver does
  // not feed more input before the previous async work completes.
  bool needsInput() const override {
    return !noMoreInput_ && !streamPending_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 private:
  const int32_t count_; // N value of TopN

  std::shared_ptr<const core::TopNNode> topNNode_;
  std::vector<cudf::size_type> sortKeys_;
  std::vector<cudf::order> columnOrder_;
  std::vector<cudf::null_order> nullOrder_;

  // When doSync=false the caller is responsible for keeping topNBatches alive
  // until the returned CudfVector's stream has completed (use
  // pendingReleaseBatches_ + cudaLaunchHostFunc for this).
  CudfVectorPtr mergeTopK(
      std::vector<CudfVectorPtr> topNBatches,
      int32_t k,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr,
      bool doSync = true);

  CudfVectorPtr getTopKBatch(CudfVectorPtr cudfInput, int32_t k);
  std::unique_ptr<cudf::table> getTopK(
      cudf::table_view const& values,
      int32_t k,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr);

  // As the inputs are added to TopN operator, we use topNBatches_
  // (a vector of CudfVectorPtrs) to keep track of the topN rows of each input.
  // We only update the topNBatches_ if number of batches >= 5 and number of
  // rows in topNBatches_ >= count_. Once all inputs are available, we concat
  // the topNBatches_ and get the topN rows.
  // config value kCudfTopNBatchSize is maximum number of batches to hold.
  std::vector<CudfVectorPtr> topNBatches_;
  int32_t kBatchSize_{5};
  bool finished_ = false;

  // --- Async GPU stream state (Phase 2 lock-free pattern) ---
  //
  // Lifecycle:
  //   addInput / getOutput (state D): set streamPending_=true, enqueue
  //     cudaLaunchHostFunc.
  //   CUDA callback thread: clear streamPending_, set streamDone_=true,
  //     fulfill streamPromise_ (wakes Velox driver).
  //   isBlocked(): returns kWaitForStream + future while streamPending_.
  //   getOutput (state A) / addInput (guard): consume streamDone_, return
  //     pendingOutput_.
  bool streamPending_{false};
  bool streamDone_{false};
  ContinuePromise streamPromise_{ContinuePromise::makeEmpty()};

  // Result produced asynchronously by getOutput state D; returned in state A.
  RowVectorPtr pendingOutput_;

  // Input batches that were moved into an async merge; kept alive here until
  // the CUDA stream callback fires and it is safe to free device memory.
  std::vector<CudfVectorPtr> pendingReleaseBatches_;
};
} // namespace facebook::velox::cudf_velox
