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
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"

#include <fmt/ranges.h>
#include <limits>
#include <thread>

namespace facebook::velox::ucx_exchange {
namespace {

void saturatingAdd(uint64_t& value, uint64_t increment) {
  const auto maximum = std::numeric_limits<uint64_t>::max();
  value = increment > maximum - value ? maximum : value + increment;
}

void mergeBackpressureMetrics(
    UcxExchangeSource::BackpressureMetrics& aggregate,
    const UcxExchangeSource::BackpressureMetrics& metrics) {
  saturatingAdd(aggregate.pauseCount, metrics.pauseCount);
  saturatingAdd(aggregate.resumeCount, metrics.resumeCount);
  saturatingAdd(
      aggregate.receiveCreditWaitCount, metrics.receiveCreditWaitCount);
  saturatingAdd(aggregate.pausedNanos, metrics.pausedNanos);
}

} // namespace

void UcxExchangeClient::addRemoteTaskId(std::string_view remoteTaskId) {
  std::shared_ptr<UcxExchangeSource> toClose;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    bool duplicate = !remoteTaskIds_.insert(std::string{remoteTaskId}).second;
    if (duplicate) {
      // Do not add sources twice. Presto protocol may add duplicate sources
      // and the task updates have no guarantees of arriving in order.
      return;
    }

    std::shared_ptr<UcxExchangeSource> source;
    source = UcxExchangeSource::create(taskId_, remoteTaskId, queue_);

    if (closed_) {
      toClose = std::move(source);
    } else {
      sources_.push_back(source);
      queue_->addSourceLocked();
      source->setRegistered();
      VLOG(3) << "@" << taskId_
              << " Added remote split for task: " << remoteTaskId;
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
    const auto metrics = toClose->backpressureMetrics();
    std::lock_guard<std::mutex> l(queue_->mutex());
    mergeClosedSourceMetricsLocked(metrics);
  }
}

void UcxExchangeClient::noMoreRemoteTasks() {
  VLOG(3) << "@" << taskId_ << " UcxExchangeClient::noMoreRemoteTasks called.";
  queue_->noMoreSources();
}

bool UcxExchangeClient::waitForSourcesPrepared(
    std::chrono::milliseconds timeout,
    const std::function<bool()>& cancelled,
    std::string* detail) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    if (cancelled && cancelled()) {
      if (detail != nullptr) {
        *detail = "receiver PREPARE was cancelled";
      }
      return false;
    }
    size_t sourceCount = 0;
    std::vector<std::string> pending;
    {
      std::lock_guard<std::mutex> lock(queue_->mutex());
      if (queue_->isInError()) {
        if (detail != nullptr) {
          *detail = "exchange source handshake failed";
        }
        return false;
      }
      if (closed_) {
        if (detail != nullptr) {
          *detail = "exchange client closed during PREPARE";
        }
        return false;
      }
      sourceCount = sources_.size();
      for (const auto& source : sources_) {
        if (!source->isReceiverPrepared()) {
          pending.push_back(
              fmt::format(
                  "{}(state={})",
                  source->toString(),
                  static_cast<uint32_t>(source->receiverState())));
        }
      }
    }
    if (sourceCount > 0 && pending.empty()) {
      return true;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      if (detail != nullptr) {
        *detail = sourceCount == 0 ? "no UCX exchange sources were registered"
                                   : fmt::format(
                                         "{} of {} source(s) not prepared: {}",
                                         pending.size(),
                                         sourceCount,
                                         fmt::join(pending, ", "));
      }
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void UcxExchangeClient::close() {
  std::vector<std::shared_ptr<UcxExchangeSource>> sources;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (closed_) {
      return;
    }
    closed_ = true;
    // Keep sources_ visible to concurrent stats() calls until close() settles
    // every active pause and atomically replaces the live sources with their
    // cumulative metrics.
    sources = sources_;
  }

  // Outside of mutex.
  UcxExchangeSource::BackpressureMetrics closedMetrics;
  for (auto& source : sources) {
    source->close();
    mergeBackpressureMetrics(closedMetrics, source->backpressureMetrics());
  }
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    mergeClosedSourceMetricsLocked(closedMetrics);
    sources_.clear();
  }
  queue_->close();
}

folly::F14FastMap<std::string, RuntimeMetric> UcxExchangeClient::stats() const {
  std::vector<std::shared_ptr<UcxExchangeSource>> sources;
  UcxExchangeSource::BackpressureMetrics sourceMetrics;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    sources = sources_;
    sourceMetrics = closedSourceMetrics_;
  }
  const auto queueStats = queue_->metricsSnapshot();

  for (const auto& source : sources) {
    mergeBackpressureMetrics(sourceMetrics, source->backpressureMetrics());
  }

  folly::F14FastMap<std::string, RuntimeMetric> stats;
  stats.insert_or_assign(kMetricQueueSize, RuntimeMetric(queueStats.queueSize));
  stats.insert_or_assign(
      kMetricCurrentQueuedBytes,
      RuntimeMetric(
          queueStats.queuedBytes, RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricCurrentPendingReceiveBytes,
      RuntimeMetric(
          queueStats.pendingReceiveBytes, RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricCurrentInflightReceiveBytes,
      RuntimeMetric(
          queueStats.inFlightBytes, RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricPeakQueuedBytes,
      RuntimeMetric(
          queueStats.peakQueuedBytes, RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricPeakInflightReceiveBytes,
      RuntimeMetric(
          queueStats.peakInflightReceiveBytes,
          RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricMaxInflightReceiveBytes,
      RuntimeMetric(
          queueStats.maxInflightReceiveBytes,
          RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricReceivedTables,
      RuntimeMetric(queueStats.receivedTables));
  stats.insert_or_assign(
      kMetricAverageReceivedTableBytes,
      RuntimeMetric(
          queueStats.averageReceivedTableBytes,
          RuntimeCounter::Unit::kBytes));
  stats.insert_or_assign(
      kMetricBackpressurePauseCount,
      RuntimeMetric(saturateCast(sourceMetrics.pauseCount)));
  stats.insert_or_assign(
      kMetricBackpressureResumeCount,
      RuntimeMetric(saturateCast(sourceMetrics.resumeCount)));
  stats.insert_or_assign(
      kMetricReceiveCreditWaitCount,
      RuntimeMetric(saturateCast(sourceMetrics.receiveCreditWaitCount)));
  stats.insert_or_assign(
      kMetricBackpressurePausedNanos,
      RuntimeMetric(
          saturateCast(sourceMetrics.pausedNanos),
          RuntimeCounter::Unit::kNanos));
  return stats;
}

void UcxExchangeClient::mergeClosedSourceMetricsLocked(
    const UcxExchangeSource::BackpressureMetrics& metrics) {
  mergeBackpressureMetrics(closedSourceMetrics_, metrics);
}

PackedTableWithStreamPtr
UcxExchangeClient::next(int consumerId, bool* atEnd, ContinueFuture* future) {
  VLOG(3) << "@" << taskId_ << " UcxExchangeClient::next called for consumerId "
          << consumerId;
  PackedTableWithStreamPtr data;
  ContinuePromise stalePromise = ContinuePromise::makeEmpty();
  std::vector<std::shared_ptr<UcxExchangeSource>> sourcesToResume;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (closed_) {
      *atEnd = true;
      return data;
    }

    // not closed, get packed table from the queue.
    *atEnd = false;
    data = queue_->dequeueLocked(consumerId, atEnd, future, &stalePromise);
    if (*atEnd) {
      // queue is closed!
      return data;
    }

    // Revive backpressured sources on the empty/blocking path. When the queue
    // has drained and next() is about to hand back a kWaitForProducer future,
    // any source this client parked under backpressure must be woken now: no
    // other thread revives a dormant source, and the consumer will not call
    // next() again with data to trigger the data-path resume below. A consumer
    // draining multiple exchanges (e.g. TPC-H Q17's correlated-aggregate join)
    // otherwise hangs forever on a source that stopped reposting receives
    // (observed as blockedWaitForProducer / WaitingForMetadata). The CAS in
    // resumeFromBackpressure() no-ops a non-dormant source, and an empty queue
    // can never warrant backpressure, so this is always safe.
    if (data == nullptr &&
        queue_->sizeLocked() <= UcxExchangeSource::kBackpressureLowWaterMark) {
      sourcesToResume.assign(sources_.begin(), sources_.end());
    }

    // TODO: Review this primitive form of flow control.
    // Maybe need to inspect the #bytes rather than the #tables?
    // Don't request more data when queue size exceeds the configured limit.
    // NOTE: This check is currently a no-op because the UCX exchange is
    // push-based — there is no mechanism to "request" or "not request" more
    // data. The server pushes unconditionally. Real backpressure is
    // implemented in UcxExchangeSource::process() (ReadyToReceive state).
    if (data != nullptr && queue_->sizeLocked() > maxQueuedColumns_) {
      if (!inFlowControl_) {
        inFlowControl_ = true;
        VLOG(1) << "[FLOW-CTRL] @" << taskId_ << " consumer=" << consumerId
                << " entering flow control"
                << " queueSize=" << queue_->sizeLocked()
                << " maxQueued=" << maxQueuedColumns_;
      }
      return data;
    } else if (inFlowControl_ && data != nullptr) {
      inFlowControl_ = false;
      VLOG(1) << "[FLOW-CTRL] @" << taskId_ << " consumer=" << consumerId
              << " leaving flow control"
              << " queueSize=" << queue_->sizeLocked()
              << " maxQueued=" << maxQueuedColumns_;
    }

    // Per-stage progress counters.
    if (data != nullptr) {
      ++totalDequeued_;
      if (totalDequeued_ % 1000 == 0) {
        VLOG(1) << "[PROGRESS] @" << taskId_ << " consumer=" << consumerId
                << " dequeued=" << totalDequeued_
                << " queueSize=" << queue_->sizeLocked()
                << " queueBytes=" << queue_->totalBytesLocked();
      }
    }

    // Collect sources that need resuming while holding the lock.
    // We call resumeFromBackpressure() outside the lock to avoid a
    // lock-ordering hazard: it acquires WorkQueue::mutex_ via
    // addToWorkQueue(), and holding queue_->mutex_ here would impose
    // queue_->mutex_ → WorkQueue::mutex_ ordering.
    if (data != nullptr &&
        queue_->sizeLocked() <= UcxExchangeSource::kBackpressureLowWaterMark) {
      sourcesToResume.assign(sources_.begin(), sources_.end());
    }
  }

  // Outside of lock: resume backpressured sources and fulfill stale promise.
  for (auto& source : sourcesToResume) {
    source->resumeFromBackpressure();
  }
  if (stalePromise.valid()) {
    stalePromise.setValue();
  }
  return data;
}

UcxExchangeClient::~UcxExchangeClient() {
  close();
}

std::string UcxExchangeClient::toString() const {
  std::stringstream out;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    for (auto& source : sources_) {
      out << source->toString() << std::endl;
    }
  }
  return out.str();
}

folly::dynamic UcxExchangeClient::toJson() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["taskId"] = taskId_;
  obj["closed"] = closed_;
  folly::dynamic clientsObj = folly::dynamic::object;
  int index = 0;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    for (auto& source : sources_) {
      clientsObj[std::to_string(index++)] = source->toJson();
    }
  }
  obj["clients"] = clientsObj;
  return obj;
}

} // namespace facebook::velox::ucx_exchange
