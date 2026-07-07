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

#include "velox/experimental/ucx-exchange/UcxExchangeQueue.h"
#include "velox/experimental/ucx-exchange/UcxExchangeSource.h"

#include <chrono>
#include <functional>
#include <string_view>
#include <unordered_set>

namespace facebook::velox::ucx_exchange {

// Handle for a set of producers. This may be shared by multiple UcxExchanges,
// one per consumer thread.
class UcxExchangeClient
    : public std::enable_shared_from_this<UcxExchangeClient> {
 public:
  // used for some primitive type of flow control, limits the size of elements
  // in the UcxExchangeQueue
  static constexpr int32_t kDefaultMaxQueuedColumns = 32;
  static constexpr std::chrono::milliseconds kRequestDataMaxWait{100};
  static constexpr char kMetricQueueSize[] =
      "ucxExchangeQueue.currentSize";
  static constexpr char kMetricCurrentQueuedBytes[] =
      "ucxExchangeQueue.currentQueuedBytes";
  static constexpr char kMetricCurrentPendingReceiveBytes[] =
      "ucxExchangeQueue.currentPendingReceiveBytes";
  static constexpr char kMetricCurrentInflightReceiveBytes[] =
      "ucxExchangeQueue.currentInflightReceiveBytes";
  static constexpr char kMetricPeakQueuedBytes[] =
      "ucxExchangeQueue.peakQueuedBytes";
  static constexpr char kMetricPeakInflightReceiveBytes[] =
      "ucxExchangeQueue.peakInflightReceiveBytes";
  static constexpr char kMetricMaxInflightReceiveBytes[] =
      "ucxExchangeQueue.maxInflightReceiveBytes";
  static constexpr char kMetricReceivedTables[] =
      "ucxExchangeQueue.receivedTables";
  static constexpr char kMetricAverageReceivedTableBytes[] =
      "ucxExchangeQueue.averageReceivedTableBytes";
  static constexpr char kMetricBackpressurePauseCount[] =
      "ucxExchangeSource.backpressurePauseCount";
  static constexpr char kMetricBackpressureResumeCount[] =
      "ucxExchangeSource.backpressureResumeCount";
  static constexpr char kMetricReceiveCreditWaitCount[] =
      "ucxExchangeSource.receiveCreditWaitCount";
  static constexpr char kMetricBackpressurePausedNanos[] =
      "ucxExchangeSource.backpressurePausedNanos";

  UcxExchangeClient(
      std::string taskId,
      int destination,
      int32_t numberOfConsumers,
      int64_t maxInflightReceiveBytesPerClient,
      int32_t requestDataSizesMaxWaitSec = 10)
      : taskId_{std::move(taskId)},
        destination_(destination),
        maxQueuedColumns_(kDefaultMaxQueuedColumns),
        kRequestDataSizesMaxWaitSec_(requestDataSizesMaxWaitSec),
        queue_(std::make_shared<UcxExchangeQueue>(
            numberOfConsumers, maxInflightReceiveBytesPerClient)) {
    VELOX_CHECK_GE(
        destination, 0, "Exchange client destination must not be negative");
    VLOG(1) << "[UCX_RECEIVE_BUDGET] task=" << taskId_
            << " destination=" << destination_
            << " maxInflightReceiveBytesPerClient="
            << queue_->maxInflightReceiveBytes();
  }

  ~UcxExchangeClient();

  // Creates a UCX exchange source and starts fetching data from the specified
  // upstream task. If 'close' has been called already, creates an exchange
  // source and immediately closes it to notify the upstream task that data is
  // no longer needed. Repeated calls with the same 'taskId' are ignored.
  void addRemoteTaskId(std::string_view remoteTaskId);

  void noMoreRemoteTasks();

  /// Wait until all registered sources have completed their receiver
  /// handshakes. This is a PREPARE-only control-plane operation; data remains
  /// blocked because MPP driver execution is still gated.
  bool waitForSourcesPrepared(
      std::chrono::milliseconds timeout,
      const std::function<bool()>& cancelled,
      std::string* detail = nullptr) const;

  // Closes all exchange sources.
  void close();

  // Returns runtime statistics aggregated across all of the exchange sources.
  // ExchangeClient is expected to report background CPU time by including a
  // runtime metric named Operator::kBackgroundCpuTimeNanos.
  folly::F14FastMap<std::string, RuntimeMetric> stats() const;

  const std::shared_ptr<UcxExchangeQueue>& queue() const {
    return queue_;
  }

  /// Returns a PackedTableWithStream object from the queue or null.
  ///
  /// If no data is available returns a nullptr and sets 'atEnd' to true if no
  /// more data is expected. If data is still expected, sets 'atEnd' to false
  /// and sets 'future' to a Future that will complete when data arrives.
  ///
  PackedTableWithStreamPtr
  next(int consumerId, bool* atEnd, ContinueFuture* future);

  std::string toString() const;

  folly::dynamic toJson() const;

  std::chrono::seconds requestDataSizesMaxWaitSec() const {
    return kRequestDataSizesMaxWaitSec_;
  }

  const std::unordered_set<std::string>& getRemoteTaskIdList() const {
    return remoteTaskIds_;
  }

 private:
  friend class UcxExchangeClientTestPeer;

  void mergeClosedSourceMetricsLocked(
      const UcxExchangeSource::BackpressureMetrics& metrics);

  // Handy for ad-hoc logging.
  const std::string taskId_;
  const int destination_;
  const int32_t maxQueuedColumns_;
  const std::chrono::seconds kRequestDataSizesMaxWaitSec_;

  const std::shared_ptr<UcxExchangeQueue> queue_;

  std::unordered_set<std::string> remoteTaskIds_;
  std::vector<std::shared_ptr<UcxExchangeSource>> sources_;
  // Cumulative source metrics survive source retirement and client close.
  // Guarded by queue_->mutex().
  UcxExchangeSource::BackpressureMetrics closedSourceMetrics_;
  bool closed_{false};

  // Total number of packed_clumns in flight.
  int64_t totalPendingColumns_{0};

  // Diagnostic counters for progress and flow control.
  int64_t totalDequeued_{0};
  bool inFlowControl_{false};
};

} // namespace facebook::velox::ucx_exchange
