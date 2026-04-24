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
#include "velox/experimental/cudf/exchange/InProcessChannel.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::cudf_velox {

exec::BlockingReason InProcessChannel::push(
    std::shared_ptr<CudfVector> vec,
    ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(vec);
  std::vector<ContinuePromise> consumersToWake;
  bool blocked = false;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (closed_.load(std::memory_order_relaxed)) {
      // Producer push after close: drop silently. Consumers have either
      // finished or aborted; enqueueing more would leak memory.
      return exec::BlockingReason::kNotBlocked;
    }
    VELOX_CHECK(
        !noMoreData_.load(std::memory_order_relaxed),
        "InProcessChannel::push after noMoreData");

    const int64_t bytes = static_cast<int64_t>(vec->estimateFlatSize());
    bufferedBytes_ += bytes;
    queue_.push_back(std::move(vec));
    consumersToWake = std::move(consumerPromises_);

    if (bufferedBytes_ >= maxBufferBytes_) {
      producerPromises_.emplace_back("InProcessChannel::push");
      *future = producerPromises_.back().getSemiFuture();
      blocked = true;
    }
  }
  // Wake outside lock.
  for (auto& p : consumersToWake) {
    p.setValue();
  }
  return blocked ? exec::BlockingReason::kWaitForConsumer
                 : exec::BlockingReason::kNotBlocked;
}

void InProcessChannel::noMoreData() {
  std::vector<ContinuePromise> consumersToWake;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (noMoreData_.exchange(true, std::memory_order_release)) {
      return; // idempotent
    }
    consumersToWake = std::move(consumerPromises_);
  }
  for (auto& p : consumersToWake) {
    p.setValue();
  }
}

void InProcessChannel::close() {
  std::vector<ContinuePromise> consumers;
  std::vector<ContinuePromise> producers;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (closed_.exchange(true, std::memory_order_release)) {
      return;
    }
    queue_.clear();
    bufferedBytes_ = 0;
    consumers = std::move(consumerPromises_);
    producers = std::move(producerPromises_);
  }
  for (auto& p : consumers) {
    p.setValue();
  }
  for (auto& p : producers) {
    p.setValue();
  }
}

std::vector<std::shared_ptr<CudfVector>> InProcessChannel::pull(
    uint32_t maxBatches,
    bool* atEnd,
    ContinueFuture* future) {
  std::vector<std::shared_ptr<CudfVector>> out;
  std::vector<ContinuePromise> producersToWake;
  {
    std::lock_guard<std::mutex> l(mu_);
    *atEnd = false;

    if (closed_.load(std::memory_order_relaxed)) {
      *atEnd = true;
      return out;
    }

    // Drain up to maxBatches.
    out.reserve(std::min<size_t>(maxBatches, queue_.size()));
    while (!queue_.empty() && out.size() < maxBatches) {
      auto& front = queue_.front();
      bufferedBytes_ -= static_cast<int64_t>(front->estimateFlatSize());
      out.push_back(std::move(front));
      queue_.pop_front();
    }

    if (!out.empty() && bufferedBytes_ < maxBufferBytes_) {
      producersToWake = std::move(producerPromises_);
    }

    if (out.empty()) {
      if (noMoreData_.load(std::memory_order_acquire)) {
        *atEnd = true;
      } else {
        consumerPromises_.emplace_back("InProcessChannel::pull");
        *future = consumerPromises_.back().getSemiFuture();
      }
    } else if (queue_.empty() && noMoreData_.load(std::memory_order_acquire)) {
      // Returned the last batch and producer is done. Signal end-of-stream
      // so the caller doesn't keep polling.
      *atEnd = true;
    }
  }
  for (auto& p : producersToWake) {
    p.setValue();
  }
  return out;
}

// --- Registry ---

InProcessChannelRegistry& InProcessChannelRegistry::get() {
  static InProcessChannelRegistry instance;
  return instance;
}

std::shared_ptr<InProcessChannel> InProcessChannelRegistry::registerChannel(
    const std::string& taskId,
    int destination,
    int64_t maxBufferBytes) {
  std::lock_guard<std::mutex> l(mu_);
  Key key{taskId, destination};
  auto it = channels_.find(key);
  if (it != channels_.end()) {
    return it->second;
  }
  auto ch = std::make_shared<InProcessChannel>(maxBufferBytes);
  channels_.emplace(std::move(key), ch);
  return ch;
}

std::shared_ptr<InProcessChannel> InProcessChannelRegistry::getChannel(
    const std::string& taskId,
    int destination) {
  std::lock_guard<std::mutex> l(mu_);
  auto it = channels_.find(Key{taskId, destination});
  if (it == channels_.end()) {
    return nullptr;
  }
  return it->second;
}

void InProcessChannelRegistry::removeTask(const std::string& taskId) {
  std::vector<std::shared_ptr<InProcessChannel>> toClose;
  {
    std::lock_guard<std::mutex> l(mu_);
    for (auto it = channels_.begin(); it != channels_.end();) {
      if (it->first.taskId == taskId) {
        toClose.push_back(it->second);
        it = channels_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& ch : toClose) {
    ch->close();
  }
}

} // namespace facebook::velox::cudf_velox
