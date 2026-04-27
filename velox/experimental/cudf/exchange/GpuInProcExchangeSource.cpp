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
#include "velox/experimental/cudf/exchange/GpuInProcExchangeSource.h"

#include <chrono>
#include <thread>

#include <folly/executors/InlineExecutor.h>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/cudf/exchange/GpuSerializedPage.h"
#include "velox/experimental/cudf/exchange/InProcessChannel.h"

namespace facebook::velox::cudf_velox {
namespace {

/// Exchange source backed by an InProcessChannel. One instance per
/// (remoteTaskId, destination) on the consumer side. The remote taskId is
/// the producer task's ID (prefixed with "gpu-inproc://") passed in via
/// RemoteConnectorSplit.
class GpuInProcExchangeSource : public exec::ExchangeSource {
 public:
  GpuInProcExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, std::move(queue), pool) {}

  bool supportsMetrics() const override {
    return true;
  }

  folly::F14FastMap<std::string, RuntimeMetric> metrics() const override {
    return {
        {"gpuInProcExchangeSource.numPages", RuntimeMetric(numPages_)},
        {"gpuInProcExchangeSource.totalBytes",
         RuntimeMetric(totalBytes_, RuntimeCounter::Unit::kBytes)},
    };
  }

  bool shouldRequestLocked() override {
    if (atEnd_) {
      return false;
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds /*maxWait*/) override {
    auto promise = VeloxPromise<Response>("GpuInProcExchangeSource::request");
    auto future = promise.getSemiFuture();

    // Honour Velox's byte budget: ExchangeClient computes maxBytes based on
    // remaining ExchangeQueue capacity. Pulling more than that defeats
    // backpressure and lets all the producer data pile into the queue.
    // pullBytes always returns at least one batch (even if that batch alone
    // exceeds maxBytes) to avoid deadlock.
    const int64_t pullBudget = static_cast<int64_t>(maxBytes);

    auto& registry = InProcessChannelRegistry::get();
    auto channel = registry.getChannel(remoteTaskId_, destination_);

    if (channel == nullptr) {
      // Producer hasn't registered its channel yet. Return an empty-and-not-
      // atEnd response so Velox's ExchangeClient will retry on the next
      // request cycle (a short spin rather than a blocking sleep, which is
      // not safe from inside request()).
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      promise.setValue(Response{0, false, {}});
      return future;
    }

    bool atEnd = false;
    ContinueFuture waitFuture;
    auto vectors = channel->pullBytes(pullBudget, &atEnd, &waitFuture);

    if (!vectors.empty()) {
      int64_t pulledBytes = 0;
      for (auto& v : vectors) {
        if (v) pulledBytes += static_cast<int64_t>(v->estimateFlatSize());
      }
      LOG(WARNING) << "GpuInProcExchangeSource::request taskId="
                   << remoteTaskId_ << " dest=" << destination_
                   << " maxBytes=" << maxBytes
                   << " pulledBatches=" << vectors.size()
                   << " pulledBytes=" << pulledBytes;
      deliver(std::move(vectors), atEnd, std::move(promise));
      return future;
    }
    if (atEnd) {
      handleAtEnd(std::move(promise));
      return future;
    }

    // Empty + not at end: wire the future to re-issue request when producer
    // pushes. We attach a continuation that re-calls pullBytes with the
    // SAME budget, so the wakeup path also stays within Velox's contract.
    auto self = std::dynamic_pointer_cast<GpuInProcExchangeSource>(
        shared_from_this());
    VELOX_CHECK_NOT_NULL(self);
    auto prom = std::make_shared<VeloxPromise<Response>>(std::move(promise));
    std::move(waitFuture)
        .via(&folly::InlineExecutor::instance())
        .thenValue([self, channel, prom, pullBudget](auto&&) mutable {
          self->drainAfterWakeup(channel, pullBudget, std::move(*prom));
        });
    return future;
  }

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    return request(0, maxWait);
  }

  void close() override {
    // Nothing to do: each request()'s promise is owned by the callback chain,
    // and the underlying channel is torn down via
    // InProcessChannelRegistry::removeTask().
  }

 private:
  void drainAfterWakeup(
      std::shared_ptr<InProcessChannel> channel,
      int64_t pullBudget,
      VeloxPromise<Response> promise) {
    bool atEnd = false;
    ContinueFuture waitFuture;
    auto vectors = channel->pullBytes(pullBudget, &atEnd, &waitFuture);
    if (!vectors.empty()) {
      deliver(std::move(vectors), atEnd, std::move(promise));
      return;
    }
    if (atEnd) {
      handleAtEnd(std::move(promise));
      return;
    }
    // Still empty and not at end; re-arm with the same budget.
    auto self = std::dynamic_pointer_cast<GpuInProcExchangeSource>(
        shared_from_this());
    VELOX_CHECK_NOT_NULL(self);
    auto prom = std::make_shared<VeloxPromise<Response>>(std::move(promise));
    std::move(waitFuture)
        .via(&folly::InlineExecutor::instance())
        .thenValue([self, channel, prom, pullBudget](auto&&) mutable {
          self->drainAfterWakeup(channel, pullBudget, std::move(*prom));
        });
  }

  /// Wrap the pulled CudfVectors into GpuSerializedPages and enqueue them
  /// into the Exchange-side queue. This mirrors what LocalGpuExchangeSource
  /// does, minus the OutputBufferManager round-trip.
  void deliver(
      std::vector<std::shared_ptr<CudfVector>> vectors,
      bool atEnd,
      VeloxPromise<Response> promise) {
    int64_t totalBytes = 0;
    int64_t numDataPages = 0;
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      for (auto& v : vectors) {
        if (v == nullptr) {
          continue;
        }
        const auto bytes = static_cast<int64_t>(v->estimateFlatSize());
        totalBytes += bytes;
        ++numDataPages;
        auto page = std::make_unique<GpuSerializedPage>(std::move(v));
        queue_->enqueueLocked(std::move(page), queuePromises);
      }
      if (atEnd) {
        queue_->enqueueLocked(nullptr, queuePromises);
        atEnd_ = true;
      }
      sequence_ += numDataPages;
    }
    for (auto& p : queuePromises) {
      p.setValue();
    }
    numPages_ += numDataPages;
    totalBytes_ += totalBytes;
    if (!promise.isFulfilled()) {
      promise.setValue(Response{totalBytes, atEnd, {}});
    }
  }

  void handleAtEnd(VeloxPromise<Response> promise) {
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      if (!atEnd_) {
        queue_->enqueueLocked(nullptr, queuePromises);
        atEnd_ = true;
      }
    }
    for (auto& p : queuePromises) {
      p.setValue();
    }
    if (!promise.isFulfilled()) {
      promise.setValue(Response{0, true, {}});
    }
  }

  int64_t numPages_{0};
  int64_t totalBytes_{0};
};

} // namespace

std::shared_ptr<exec::ExchangeSource> createGpuInProcExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  if (!isInProcessTaskId(taskId)) {
    return nullptr;
  }
  return std::make_shared<GpuInProcExchangeSource>(
      taskId, destination, std::move(queue), pool);
}

} // namespace facebook::velox::cudf_velox
