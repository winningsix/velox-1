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
#include "velox/experimental/ucx-exchange/IntraNodeTransferRegistry.h"
#include <glog/logging.h>
#include <limits>
#include <stdexcept>

namespace facebook::velox::ucx_exchange {

std::string_view intraNodeTransferStatusName(IntraNodeTransferStatus status) {
  switch (status) {
    case IntraNodeTransferStatus::kData:
      return "DATA";
    case IntraNodeTransferStatus::kEnd:
      return "END";
    case IntraNodeTransferStatus::kCancelled:
      return "CANCELLED";
    case IntraNodeTransferStatus::kTimedOut:
      return "TIMED_OUT";
    case IntraNodeTransferStatus::kAlreadyConsumed:
      return "ALREADY_CONSUMED";
  }
  return "UNKNOWN";
}

/* static */
std::shared_ptr<IntraNodeTransferRegistry>
IntraNodeTransferRegistry::getInstance() {
  // In C++11, the static local variable is guaranteed to only be initialized
  // once even in a multi-threaded context.
  static std::shared_ptr<IntraNodeTransferRegistry> instance =
      std::shared_ptr<IntraNodeTransferRegistry>(
          new IntraNodeTransferRegistry());
  return instance;
}

std::shared_future<void> IntraNodeTransferRegistry::publish(
    const IntraNodeTransferKey& key,
    std::shared_ptr<cudf::packed_columns> data,
    rmm::cuda_stream_view stream,
    bool atEnd,
    std::function<void()> onRetrieved) {
  std::shared_ptr<IntraNodeTransferEntry> entry;
  bool entryExisted{false};
  size_t registrySize{0};

  bool cancelled = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    // Unknown, retired, and stale task epochs fail closed. A delayed publisher
    // from an old incarnation can never attach to a replacement task's entry.
    if (!isActiveLocked(key.taskToken)) {
      cancelled = true;
    } else {
      auto it = registry_.find(key);
      entryExisted = (it != registry_.end());
      if (entryExisted) {
        entry = it->second;
      } else {
        entry = std::make_shared<IntraNodeTransferEntry>();
        registry_[key] = entry;
      }
      registrySize = registry_.size();
    }
  }

  if (cancelled) {
    VLOG(2) << "[INTRA-REG] publish skipped (task cancelled): task="
            << key.taskToken.taskId << " epoch=" << key.taskToken.epoch
            << " dest=" << key.destination << " seq=" << key.sequenceNumber;
    if (onRetrieved) {
      try {
        onRetrieved();
      } catch (...) {
        // A completion observer cannot reopen a cancelled publication.
      }
    }
    return readyFuture();
  }

  CompletionActions actions;
  std::shared_future<void> future;
  {
    std::lock_guard<std::mutex> entryLock(entry->entryMutex);
    future = entry->retrievedFuture;
    if (entry->fulfilled) {
      if (onRetrieved) {
        actions.retrievedWakeups.push_back(std::move(onRetrieved));
      }
    } else {
      if (onRetrieved) {
        entry->retrievedCallbacks.push_back(std::move(onRetrieved));
      }
      // Duplicate publication shares the original entry and future. It never
      // overwrites data already exposed to a consumer.
      if (!entry->published) {
        entry->data = std::move(data);
        entry->stream = stream;
        entry->atEnd = atEnd;
        entry->published = true;
        entry->ready = true;
        actions.notifyDataAvailable = true;
        actions.sourceWakeups.swap(entry->wakeCallbacks);
      }
    }
  }

  if (actions.notifyDataAvailable) {
    entry->dataAvailable.notify_all();
  }
  invokeAll(actions.sourceWakeups);
  invokeAll(actions.retrievedWakeups);

  VLOG(2) << "[INTRA-REG] publish: task=" << key.taskToken.taskId
          << " epoch=" << key.taskToken.epoch << " dest=" << key.destination
          << " seq=" << key.sequenceNumber << " atEnd=" << atEnd
          << " entryExisted=" << entryExisted
          << " registrySize=" << registrySize;

  return future;
}

std::optional<IntraNodeTransferResult> IntraNodeTransferRegistry::poll(
    const IntraNodeTransferKey& key) {
  std::shared_ptr<IntraNodeTransferEntry> entry;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!isActiveLocked(key.taskToken)) {
      VLOG(2) << "[INTRA-REG] poll cancelled: task="
              << key.taskToken.taskId << " epoch=" << key.taskToken.epoch
              << " dest=" << key.destination
              << " seq=" << key.sequenceNumber;
      return IntraNodeTransferResult{
          nullptr,
          rmm::cuda_stream_default,
          IntraNodeTransferStatus::kCancelled};
    }
    auto it = registry_.find(key);
    if (it == registry_.end()) {
      return std::nullopt;
    }
    entry = it->second;
  }

  CompletionActions actions;
  std::optional<IntraNodeTransferResult> result;
  {
    std::lock_guard<std::mutex> entryLock(entry->entryMutex);
    result = consumeOnceLocked(*entry, actions);
  }
  if (!result.has_value()) {
    return std::nullopt;
  }

  eraseIfSame(key, entry);
  invokeAll(actions.retrievedWakeups);

  VLOG(2) << "[INTRA-REG] poll hit: task=" << key.taskToken.taskId
          << " epoch=" << key.taskToken.epoch << " dest=" << key.destination
          << " seq=" << key.sequenceNumber << " status="
          << intraNodeTransferStatusName(result->status);
  return result;
}

bool IntraNodeTransferRegistry::registerWaiter(
    const IntraNodeTransferKey& key,
    std::function<void()> wake) {
  if (!wake) {
    throw std::invalid_argument("Intra-node waiter callback must be non-empty");
  }
  std::shared_ptr<IntraNodeTransferEntry> entry;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!isActiveLocked(key.taskToken)) {
      return true;
    }
    auto it = registry_.find(key);
    if (it != registry_.end()) {
      entry = it->second;
    } else {
      entry = std::make_shared<IntraNodeTransferEntry>();
      registry_[key] = entry;
    }
  }

  std::lock_guard<std::mutex> entryLock(entry->entryMutex);
  if (entry->ready || entry->fulfilled) {
    return true;
  }
  entry->wakeCallbacks.push_back(std::move(wake));
  return false;
}

IntraNodeTransferResult IntraNodeTransferRegistry::waitFor(
    const IntraNodeTransferKey& key,
    std::chrono::milliseconds timeout) {
  std::shared_ptr<IntraNodeTransferEntry> entry;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!isActiveLocked(key.taskToken)) {
      return {
          nullptr,
          rmm::cuda_stream_default,
          IntraNodeTransferStatus::kCancelled};
    }
    auto it = registry_.find(key);
    if (it != registry_.end()) {
      entry = it->second;
    } else {
      entry = std::make_shared<IntraNodeTransferEntry>();
      registry_[key] = entry;
    }
  }

  CompletionActions actions;
  std::optional<IntraNodeTransferResult> result;
  {
    std::unique_lock<std::mutex> entryLock(entry->entryMutex);
    if (!entry->ready && !entry->fulfilled) {
      const auto signalled = entry->dataAvailable.wait_for(
          entryLock,
          timeout,
          [&entry]() { return entry->ready || entry->fulfilled; });
      if (!signalled) {
        VLOG(0) << "Timeout waiting for intra-node transfer: "
                << key.taskToken.taskId << " epoch=" << key.taskToken.epoch
                << " dest=" << key.destination
                << " seq=" << key.sequenceNumber;
        return {
            nullptr,
            rmm::cuda_stream_default,
            IntraNodeTransferStatus::kTimedOut};
      }
    }
    result = consumeOnceLocked(*entry, actions);
  }

  if (!result.has_value()) {
    // A fulfilled entry with no consumable payload is terminal for a duplicate
    // consumer. Fail closed instead of waiting again on a one-shot key.
    result = IntraNodeTransferResult{
        nullptr,
        rmm::cuda_stream_default,
        IntraNodeTransferStatus::kAlreadyConsumed};
  }
  eraseIfSame(key, entry);
  invokeAll(actions.retrievedWakeups);
  return *result;
}

std::optional<IntraNodeTransferResult> IntraNodeTransferRegistry::retrieve(
    const IntraNodeTransferKey& key) {
  std::shared_ptr<IntraNodeTransferEntry> entry;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!isActiveLocked(key.taskToken)) {
      return IntraNodeTransferResult{
          nullptr,
          rmm::cuda_stream_default,
          IntraNodeTransferStatus::kCancelled};
    }
    auto it = registry_.find(key);
    if (it == registry_.end()) {
      return std::nullopt;
    }
    entry = it->second;
  }

  CompletionActions actions;
  std::optional<IntraNodeTransferResult> result;
  {
    std::lock_guard<std::mutex> entryLock(entry->entryMutex);
    result = consumeOnceLocked(*entry, actions);
  }
  if (!result.has_value()) {
    return std::nullopt;
  }
  eraseIfSame(key, entry);
  invokeAll(actions.retrievedWakeups);
  return result;
}

void IntraNodeTransferRegistry::cancelTask(const TaskToken& taskToken) {
  if (!taskToken) {
    return;
  }
  std::vector<std::shared_ptr<IntraNodeTransferEntry>> entries;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto active = activeTasks_.find(taskToken.taskId);
    if (active != activeTasks_.end() && active->second == taskToken.epoch) {
      activeTasks_.erase(active);
    }
    for (auto it = registry_.begin(); it != registry_.end();) {
      if (it->first.taskToken == taskToken) {
        entries.push_back(it->second);
        it = registry_.erase(it);
      } else {
        ++it;
      }
    }
  }

  for (auto& entry : entries) {
    CompletionActions actions;
    std::shared_ptr<cudf::packed_columns> discardedData;
    {
      std::lock_guard<std::mutex> entryLock(entry->entryMutex);
      entry->cancelled = true;
      discardedData = std::move(entry->data);
      entry->stream = rmm::cuda_stream_default;
      // Only a producer publication may create a successful EOS marker.
      entry->atEnd = false;
      entry->ready = true;
      actions.notifyDataAvailable = true;
      actions.sourceWakeups.swap(entry->wakeCallbacks);
      fulfillOnceLocked(*entry, actions);
    }
    // Cancellation can have multiple blocking waiters on one shared entry.
    // notify_all is required; notify_one strands the remaining waiters.
    entry->dataAvailable.notify_all();
    invokeAll(actions.sourceWakeups);
    invokeAll(actions.retrievedWakeups);
    // Release potentially expensive GPU-backed data after all registry locks.
    discardedData.reset();
  }

  VLOG(2) << "[INTRA-REG] cancelTask: task=" << taskToken.taskId
          << " epoch=" << taskToken.epoch << " entriesCleaned=" << entries.size();
}

IntraNodeTransferRegistry::TaskRegistration
IntraNodeTransferRegistry::expectTask(std::string_view taskId) {
  if (taskId.empty()) {
    throw std::invalid_argument("Intra-node task ID must be non-empty");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const std::string id{taskId};
  auto active = activeTasks_.find(id);
  if (active != activeTasks_.end()) {
    return {TaskToken{id, active->second}, false};
  }
  if (nextEpoch_ == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("Intra-node task epoch space is exhausted");
  }
  TaskToken token{id, nextEpoch_};
  activeTasks_.emplace(id, token.epoch);
  ++nextEpoch_;
  return {std::move(token), true};
}

std::optional<TaskToken> IntraNodeTransferRegistry::activeTaskToken(
    std::string_view taskId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto active = activeTasks_.find(std::string{taskId});
  if (active == activeTasks_.end()) {
    return std::nullopt;
  }
  return TaskToken{active->first, active->second};
}

void IntraNodeTransferRegistry::fulfillOnceLocked(
    IntraNodeTransferEntry& entry,
    CompletionActions& actions) {
  if (entry.fulfilled) {
    return;
  }
  entry.fulfilled = true;
  entry.retrievedPromise.set_value();
  actions.retrievedWakeups.swap(entry.retrievedCallbacks);
}

std::optional<IntraNodeTransferResult>
IntraNodeTransferRegistry::consumeOnceLocked(
    IntraNodeTransferEntry& entry,
    CompletionActions& actions) {
  if (!entry.ready) {
    return std::nullopt;
  }
  if (entry.consumed) {
    return IntraNodeTransferResult{
        nullptr,
        rmm::cuda_stream_default,
        IntraNodeTransferStatus::kAlreadyConsumed};
  }
  entry.consumed = true;
  const auto status = entry.cancelled
      ? IntraNodeTransferStatus::kCancelled
      : (entry.atEnd ? IntraNodeTransferStatus::kEnd
                     : IntraNodeTransferStatus::kData);
  IntraNodeTransferResult result{std::move(entry.data), entry.stream, status};
  fulfillOnceLocked(entry, actions);
  return result;
}

void IntraNodeTransferRegistry::invokeAll(
    std::vector<std::function<void()>>& callbacks) noexcept {
  for (auto& callback : callbacks) {
    try {
      callback();
    } catch (...) {
      // Completion is exactly-once even if an observer callback fails.
    }
  }
  callbacks.clear();
}

std::shared_future<void> IntraNodeTransferRegistry::readyFuture() {
  std::promise<void> promise;
  auto future = promise.get_future().share();
  promise.set_value();
  return future;
}

bool IntraNodeTransferRegistry::isActiveLocked(
    const TaskToken& taskToken) const {
  auto active = activeTasks_.find(taskToken.taskId);
  return active != activeTasks_.end() && active->second == taskToken.epoch;
}

void IntraNodeTransferRegistry::eraseIfSame(
    const IntraNodeTransferKey& key,
    const std::shared_ptr<IntraNodeTransferEntry>& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = registry_.find(key);
  if (it != registry_.end() && it->first.taskToken == key.taskToken &&
      it->second == entry) {
    registry_.erase(it);
  }
}

} // namespace facebook::velox::ucx_exchange
