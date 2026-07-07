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

#include <cudf/contiguous_split.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <condition_variable>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "velox/experimental/ucx-exchange/UcxTaskToken.h"

namespace facebook::velox::ucx_exchange {

/// @brief Key for identifying intra-node transfer entries in the registry.
/// Used when UcxExchangeServer and UcxExchangeSource are on the same node.
struct IntraNodeTransferKey {
  TaskToken taskToken;
  uint32_t destination;
  uint32_t sequenceNumber;

  bool operator<(const IntraNodeTransferKey& other) const {
    if (taskToken != other.taskToken)
      return taskToken < other.taskToken;
    if (destination != other.destination)
      return destination < other.destination;
    return sequenceNumber < other.sequenceNumber;
  }
};

enum class IntraNodeTransferStatus : uint8_t {
  kData,
  kEnd,
  kCancelled,
  kTimedOut,
  kAlreadyConsumed,
};

/// @brief Result from one exact intra-node transfer entry. Only kEnd is a
/// successful end-of-stream marker. Cancellation, stale epochs, timeouts, and
/// duplicate consumption remain explicit errors and must never masquerade as
/// EOS at the exchange source.
struct IntraNodeTransferResult {
  std::shared_ptr<cudf::packed_columns> data;
  rmm::cuda_stream_view stream{rmm::cuda_stream_default};
  IntraNodeTransferStatus status{IntraNodeTransferStatus::kData};
};

std::string_view intraNodeTransferStatusName(IntraNodeTransferStatus status);

/// @brief Entry in the intra-node transfer registry containing the data and
/// synchronization primitives. The server publishes data via publish() and
/// waits on retrievedFuture; the source polls via poll() and completes the
/// shared entry through the registry's single fulfillOnce path.
struct IntraNodeTransferEntry {
  IntraNodeTransferEntry()
      : retrievedFuture(retrievedPromise.get_future().share()) {}

  std::shared_ptr<cudf::packed_columns> data;
  rmm::cuda_stream_view stream{rmm::cuda_stream_default};
  bool atEnd{false}; // True if this is the end-of-stream marker
  std::promise<void> retrievedPromise; // Completed exactly once.
  std::shared_future<void> retrievedFuture;
  std::condition_variable dataAvailable; // Source waits on this for data
  std::mutex entryMutex;
  bool ready{false}; // True when data is ready to retrieve
  bool published{false};
  bool consumed{false};
  bool cancelled{false};
  bool fulfilled{false};
  // One-shot wakeups for sources that registered via registerWaiter() before
  // the data was ready, so a same-process consumer can stay dormant instead of
  // busy-polling the single-threaded Communicator work queue. Fired and cleared
  // by publish()/cancelTask(). Guarded by entryMutex.
  std::vector<std::function<void()>> wakeCallbacks;
  // One-shot wakeups for producers waiting until the source has retrieved the
  // published data. Fired and cleared when poll()/waitFor()/retrieve() consumes
  // the entry, or when cancelTask() completes it as cancelled.
  std::vector<std::function<void()>> retrievedCallbacks;
};

/// @brief Singleton registry for intra-node data transfers.
///
/// When UcxExchangeServer and UcxExchangeSource are on the same node
/// (same Communicator instance), this registry enables direct data sharing
/// without going through UCXX network transfers.
///
/// The server publishes packed_columns data to the registry, and the source
/// polls for it. Synchronization is handled via futures/promises.
class IntraNodeTransferRegistry {
 public:
  /// @brief Get the singleton instance of the registry.
  static std::shared_ptr<IntraNodeTransferRegistry> getInstance();

  // Prevent copying
  IntraNodeTransferRegistry(const IntraNodeTransferRegistry&) = delete;
  IntraNodeTransferRegistry& operator=(const IntraNodeTransferRegistry&) =
      delete;

  /// @brief Publish data for intra-node transfer with condition variable
  /// signaling. Server calls this to make data available to the source.
  /// The producer must synchronize its CUDA stream before calling publish()
  /// so that the GPU data is ready when the consumer reads it.
  /// @param key The unique key identifying this transfer (taskId, dest, seq)
  /// @param data The packed_columns data to share (nullptr for atEnd)
  /// @param atEnd True if this is the end-of-stream marker
  /// @return A future that completes when source has retrieved the data
  [[nodiscard]] std::shared_future<void> publish(
      const IntraNodeTransferKey& key,
      std::shared_ptr<cudf::packed_columns> data,
      rmm::cuda_stream_view stream,
      bool atEnd,
      std::function<void()> onRetrieved = {});

  /// @brief Non-blocking poll for intra-node transfer data.
  /// Returns immediately whether data is available or not.
  /// @param key The unique key identifying this transfer
  /// @return nullopt if not ready, or IntraNodeTransferResult if ready.
  ///         Data is nullptr when atEnd is true.
  [[nodiscard]] std::optional<IntraNodeTransferResult> poll(
      const IntraNodeTransferKey& key);

  /// @brief Register a one-shot wakeup for a same-process consumer that polled
  /// and found no data, so it can go dormant instead of busy-re-queuing on the
  /// single-threaded Communicator. @p wake is invoked exactly once when
  /// publish() (or cancelTask()) makes data available for @p key.
  /// @param key The unique key identifying this transfer
  /// @param wake Callback fired once when data becomes available or the task is
  ///        cancelled; must be safe to call from the Communicator thread.
  /// @return true if data is ALREADY available (or the task is cancelled) — the
  ///         caller should re-poll instead of going dormant; false if registered
  ///         (the caller should go dormant and will be woken via @p wake).
  [[nodiscard]] bool registerWaiter(
      const IntraNodeTransferKey& key,
      std::function<void()> wake);

  /// @brief Wait for and retrieve data from intra-node transfer registry.
  /// Source calls this - blocks until data is available or timeout.
  /// WARNING: This can block, use with caution on single-threaded contexts.
  /// @param key The unique key identifying this transfer
  /// @param timeout Maximum time to wait for data
  /// @return Explicit Data, End, Cancelled, TimedOut, or AlreadyConsumed state.
  [[nodiscard]] IntraNodeTransferResult waitFor(
      const IntraNodeTransferKey& key,
      std::chrono::milliseconds timeout);

  /// @brief Non-blocking legacy retrieval with the same explicit terminal
  /// status and exactly-once completion semantics as poll().
  /// @param key The unique key identifying this transfer
  /// @return nullopt if the entry is not ready, otherwise its one-shot result.
  std::optional<IntraNodeTransferResult> retrieve(
      const IntraNodeTransferKey& key);

  /// @brief Cancel all pending transfers for a task.
  /// Called when a producing task is removed. Subsequent poll() calls for this
  /// exact token return CANCELLED, never successful EOS.
  /// @param taskToken Exact task incarnation to cancel. A delayed cancellation
  ///        for an old epoch never removes a replacement incarnation.
  void cancelTask(const TaskToken& taskToken);

  /// Declare a live task. Unknown and retired tasks fail closed in publish,
  /// poll and waiter registration without retaining historical tombstones.
  struct TaskRegistration {
    TaskToken token;
    bool inserted{false};
  };

  /// Declare a live task or return its existing token for an idempotent repeat.
  /// Re-declaring an ID after its exact token was cancelled allocates a new
  /// non-zero epoch.
  [[nodiscard]] TaskRegistration expectTask(std::string_view taskId);

  [[nodiscard]] std::optional<TaskToken> activeTaskToken(
      std::string_view taskId) const;

 private:
  IntraNodeTransferRegistry() = default;

  struct CompletionActions {
    std::vector<std::function<void()>> sourceWakeups;
    std::vector<std::function<void()>> retrievedWakeups;
    bool notifyDataAvailable{false};
  };

  static void fulfillOnceLocked(
      IntraNodeTransferEntry& entry,
      CompletionActions& actions);
  static std::optional<IntraNodeTransferResult> consumeOnceLocked(
      IntraNodeTransferEntry& entry,
      CompletionActions& actions);
  static void invokeAll(std::vector<std::function<void()>>& callbacks) noexcept;
  static std::shared_future<void> readyFuture();

  bool isActiveLocked(const TaskToken& taskToken) const;
  void eraseIfSame(
      const IntraNodeTransferKey& key,
      const std::shared_ptr<IntraNodeTransferEntry>& entry);

  std::map<IntraNodeTransferKey, std::shared_ptr<IntraNodeTransferEntry>>
      registry_;
  // Mirrors UcxOutputQueueManager's active task incarnations. Historical
  // tokens are erased by cancelTask(); unknown epochs fail closed.
  std::unordered_map<std::string, uint64_t> activeTasks_;
  uint64_t nextEpoch_{1};
  mutable std::mutex mutex_;
};

} // namespace facebook::velox::ucx_exchange
