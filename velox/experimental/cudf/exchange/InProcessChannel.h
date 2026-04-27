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

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/future/VeloxPromise.h"
#include "velox/exec/Exchange.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

namespace facebook::velox::cudf_velox {

/// Single-process, same-GPU handoff channel for CudfVector between Velox
/// Tasks. Replaces the OutputBufferManager + SharedSerializedPage + HTTP-style
/// acknowledge lifecycle on the "gpu-inproc://" task-id path.
///
/// Scope (deliberately narrow):
///   * No serialization: stores std::shared_ptr<CudfVector> directly.
///   * Single machine, single GPU, single process -- no UCX, no NVLink, no
///     inter-node transport.
///   * One channel per (producerTaskId, destination). Consumer and producer
///     addresses by that key via InProcessChannelRegistry.
///   * Non-broadcast only in the first cut; broadcast fan-out is a follow-up.
///
/// Thread-safety: all public methods are thread-safe. Producers push,
/// consumers pull via CudfVectorPtr.
class InProcessChannel {
 public:
  explicit InProcessChannel(int64_t maxBufferBytes)
      : maxBufferBytes_(maxBufferBytes) {}

  /// Push a vector onto the queue. If pending bytes reach maxBufferBytes_,
  /// the producer is blocked: the returned blocking reason is
  /// kWaitForConsumer and 'future' is set so the caller can wait.
  exec::BlockingReason push(
      std::shared_ptr<CudfVector> vec,
      ContinueFuture* future);

  /// Producer signals no more data. Idempotent.
  void noMoreData();

  /// Close the channel (producer aborted / consumer done). Drops any queued
  /// vectors and wakes up all waiters.
  void close();

  /// Consumer pull. If at least one vector is queued, returns up to
  /// 'maxBatches' of them. If empty and producer is done, sets '*atEnd' =
  /// true. If empty and producer still active, sets 'future' and returns
  /// empty.
  std::vector<std::shared_ptr<CudfVector>> pull(
      uint32_t maxBatches,
      bool* atEnd,
      ContinueFuture* future);

  /// Consumer pull, byte-budget variant. Drains queued vectors until the
  /// accumulated estimateFlatSize() reaches 'maxBytes', or the queue is
  /// empty. To avoid starvation when a single batch exceeds maxBytes, the
  /// first batch is always returned regardless of size. atEnd / future
  /// semantics are identical to pull().
  std::vector<std::shared_ptr<CudfVector>> pullBytes(
      int64_t maxBytes,
      bool* atEnd,
      ContinueFuture* future);

  bool noMoreProducers() const {
    return noMoreData_.load(std::memory_order_acquire);
  }

 private:
  const int64_t maxBufferBytes_;

  mutable std::mutex mu_;
  std::deque<std::shared_ptr<CudfVector>> queue_;
  int64_t bufferedBytes_{0};
  std::atomic<bool> noMoreData_{false};
  std::atomic<bool> closed_{false};

  // Fulfilled whenever the buffer drops below maxBufferBytes_.
  std::vector<ContinuePromise> producerPromises_;
  // Fulfilled whenever a push / noMoreData / close happens.
  std::vector<ContinuePromise> consumerPromises_;
};

/// Global registry keyed by (producerTaskId, destination). Producer operators
/// look up their channel via registerChannel() when the Task starts (driven by
/// taskId prefix == "gpu-inproc://"); consumer ExchangeSources look it up via
/// getChannel(). Channels are removed when the query coordinator tears down
/// its tasks (removeTask()).
class InProcessChannelRegistry {
 public:
  static InProcessChannelRegistry& get();

  /// Register a channel under (taskId, destination). Reentrant: returns the
  /// existing channel if already registered.
  std::shared_ptr<InProcessChannel> registerChannel(
      const std::string& taskId,
      int destination,
      int64_t maxBufferBytes);

  /// Look up a channel. Returns nullptr if not registered (consumer may
  /// need to wait and retry while the producer spins up).
  std::shared_ptr<InProcessChannel> getChannel(
      const std::string& taskId,
      int destination);

  /// Remove all channels for a task. Closes them to unblock any waiters.
  void removeTask(const std::string& taskId);

 private:
  InProcessChannelRegistry() = default;

  struct Key {
    std::string taskId;
    int destination;

    bool operator==(const Key& other) const {
      return destination == other.destination && taskId == other.taskId;
    }
  };

  struct KeyHash {
    size_t operator()(const Key& k) const noexcept {
      return std::hash<std::string>{}(k.taskId) ^
          (std::hash<int>{}(k.destination) << 1);
    }
  };

  std::mutex mu_;
  std::unordered_map<Key, std::shared_ptr<InProcessChannel>, KeyHash>
      channels_;
};

/// Returns the task-id prefix used to route channels through
/// InProcessChannelRegistry. Producers whose taskId starts with this prefix
/// must publish to InProcessChannel instead of OutputBufferManager; consumers
/// reading from such producers use GpuInProcExchangeSource.
constexpr const char* kInProcessTaskIdPrefix = "gpu-inproc://";

/// True if the given taskId is routed via InProcessChannel.
inline bool isInProcessTaskId(const std::string& taskId) {
  const std::size_t prefixLen = std::strlen(kInProcessTaskIdPrefix);
  return taskId.size() >= prefixLen &&
      std::memcmp(taskId.data(), kInProcessTaskIdPrefix, prefixLen) == 0;
}

} // namespace facebook::velox::cudf_velox
