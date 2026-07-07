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

#include "velox/experimental/ucx-exchange/UcxTaskLifecycleRegistry.h"

#include <algorithm>
#include <utility>

namespace facebook::velox::ucx_exchange {

UcxTaskLifecycleRegistry::UcxTaskLifecycleRegistry()
    : UcxTaskLifecycleRegistry(Options{}) {}

UcxTaskLifecycleRegistry::UcxTaskLifecycleRegistry(Options options)
    : options_(std::move(options)) {
  if (options_.expectedTaskCapacity == 0 ||
      options_.pendingRequestCapacity == 0 ||
      options_.maxDestinationsPerTask == 0 ||
      options_.unknownTaskWait <= std::chrono::milliseconds::zero()) {
    throw std::invalid_argument(
        "UCX task lifecycle capacities, destination limit, and wait must be positive");
  }
}

UcxTaskLifecycleRegistry::~UcxTaskLifecycleRegistry() {
  shutdown();
}

bool UcxTaskLifecycleRegistry::expectTask(
    std::string_view taskId,
    TaskContract contract) {
  if (taskId.empty()) {
    throw std::invalid_argument("Expected UCX task ID must not be empty");
  }
  if (contract.destinationCount == 0 ||
      contract.destinationCount > options_.maxDestinationsPerTask) {
    throw std::invalid_argument(
        "Expected UCX task destination count is outside the configured limit");
  }
  std::vector<ResolvedRequest> adopted;
  bool inserted = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      throw std::logic_error("UCX task lifecycle registry is stopped");
    }
    const std::string id{taskId};
    auto expectedIt = expectedTasks_.find(id);
    if (expectedIt == expectedTasks_.end()) {
      if (expectedTasks_.size() >= options_.expectedTaskCapacity) {
        ++totalRejected_;
        throw UcxTaskLifecycleCapacityError(
            "UCX expected-task capacity is exhausted");
      }
      expectedTasks_.emplace(id, contract);
      ++totalExpected_;
      inserted = true;
    } else if (!(expectedIt->second == contract)) {
      throw std::invalid_argument(
          "UCX task was re-declared with a different output contract");
    }
    auto pendingIt = pending_.find(id);
    if (pendingIt != pending_.end()) {
      adopted.reserve(pendingIt->second.size());
      for (auto& request : pendingIt->second) {
        const bool accepted = request.destination < contract.destinationCount;
        adopted.push_back(
            ResolvedRequest{
                std::move(request.onExpected),
                std::move(request.onRejected),
                accepted ? AdmissionRejectReason::kNone
                         : AdmissionRejectReason::kInvalidDestination,
                accepted});
        if (!accepted) {
          ++totalRejected_;
        }
      }
      pendingRequestCount_ -= pendingIt->second.size();
      totalAdopted_ += std::count_if(
          adopted.begin(), adopted.end(), [](const auto& request) {
            return request.accepted;
          });
      pending_.erase(pendingIt);
    }
  }
  cv_.notify_all();
  invokeAll(adopted);
  return inserted;
}

UcxTaskLifecycleRegistry::AdmissionResult
UcxTaskLifecycleRegistry::admitRequest(
    std::string_view taskId,
    uint32_t destination,
    Callback onExpected,
    RejectCallback onRejected) {
  if (taskId.empty() || !onExpected || !onRejected) {
    throw std::invalid_argument(
        "UCX admission requires task ID and both callbacks");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const std::string id{taskId};
  auto expectedIt = expectedTasks_.find(id);
  if (expectedIt != expectedTasks_.end()) {
    if (destination >= expectedIt->second.destinationCount) {
      ++totalRejected_;
      return AdmissionResult{
          RequestDisposition::kRejected,
          AdmissionRejectReason::kInvalidDestination,
          expectedIt->second};
    }
    return AdmissionResult{
        RequestDisposition::kExpected,
        AdmissionRejectReason::kNone,
        expectedIt->second};
  }
  if (stopping_ || pendingRequestCount_ >= options_.pendingRequestCapacity) {
    ++totalRejected_;
    return AdmissionResult{
        RequestDisposition::kRejected,
        stopping_ ? AdmissionRejectReason::kShutdown
                  : AdmissionRejectReason::kCapacity,
        {}};
  }
  if (!reaperThread_.joinable()) {
    try {
      reaperThread_ = std::thread([this]() { reaperLoop(); });
    } catch (...) {
      ++totalRejected_;
      return AdmissionResult{
          RequestDisposition::kRejected, AdmissionRejectReason::kCapacity, {}};
    }
  }
  try {
    pending_[id].push_back(
        PendingRequest{
            Clock::now() + options_.unknownTaskWait,
            destination,
            std::move(onExpected),
            std::move(onRejected)});
  } catch (...) {
    auto pendingIt = pending_.find(id);
    if (pendingIt != pending_.end() && pendingIt->second.empty()) {
      pending_.erase(pendingIt);
    }
    ++totalRejected_;
    return AdmissionResult{
        RequestDisposition::kRejected, AdmissionRejectReason::kCapacity, {}};
  }
  ++pendingRequestCount_;
  ++totalDeferred_;
  cv_.notify_all();
  return AdmissionResult{
      RequestDisposition::kDeferred, AdmissionRejectReason::kNone, {}};
}

bool UcxTaskLifecycleRegistry::expandBroadcastDestinations(
    std::string_view taskId,
    uint32_t destinationCount) {
  if (destinationCount == 0 ||
      destinationCount > options_.maxDestinationsPerTask) {
    throw std::invalid_argument(
        "Expanded UCX destination count is outside the configured limit");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = expectedTasks_.find(std::string{taskId});
  if (it == expectedTasks_.end()) {
    return false;
  }
  if (it->second.outputKind != OutputKind::kBroadcast) {
    if (it->second.destinationCount != destinationCount) {
      throw std::invalid_argument(
          "Only broadcast UCX task contracts may expand destinations");
    }
    return false;
  }
  if (destinationCount < it->second.destinationCount) {
    throw std::invalid_argument(
        "UCX broadcast destination contract cannot shrink");
  }
  const bool changed = destinationCount != it->second.destinationCount;
  it->second.destinationCount = destinationCount;
  return changed;
}

std::optional<UcxTaskLifecycleRegistry::TaskContract>
UcxTaskLifecycleRegistry::taskContract(std::string_view taskId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = expectedTasks_.find(std::string{taskId});
  if (it == expectedTasks_.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool UcxTaskLifecycleRegistry::retireTask(std::string_view taskId) {
  std::vector<ResolvedRequest> expired;
  bool removed = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string id{taskId};
    removed = expectedTasks_.erase(id) > 0;
    if (removed) {
      ++totalRetired_;
    }
    auto pendingIt = pending_.find(id);
    if (pendingIt != pending_.end()) {
      expired.reserve(pendingIt->second.size());
      for (auto& request : pendingIt->second) {
        expired.push_back(
            ResolvedRequest{
                std::move(request.onExpected),
                std::move(request.onRejected),
                AdmissionRejectReason::kRetired,
                false});
      }
      pendingRequestCount_ -= pendingIt->second.size();
      totalExpired_ += pendingIt->second.size();
      pending_.erase(pendingIt);
    }
  }
  cv_.notify_all();
  invokeAll(expired);
  return removed;
}

bool UcxTaskLifecycleRegistry::isExpected(std::string_view taskId) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return expectedTasks_.count(std::string{taskId}) > 0;
}

size_t UcxTaskLifecycleRegistry::reapExpired() {
  const auto now = Clock::now();
  std::vector<ResolvedRequest> expired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto mapIt = pending_.begin(); mapIt != pending_.end();) {
      auto& requests = mapIt->second;
      auto requestIt = requests.begin();
      while (requestIt != requests.end()) {
        if (requestIt->deadline > now) {
          ++requestIt;
          continue;
        }
        expired.push_back(
            ResolvedRequest{
                std::move(requestIt->onExpected),
                std::move(requestIt->onRejected),
                AdmissionRejectReason::kExpired,
                false});
        requestIt = requests.erase(requestIt);
        --pendingRequestCount_;
        ++totalExpired_;
      }
      if (requests.empty()) {
        mapIt = pending_.erase(mapIt);
      } else {
        ++mapIt;
      }
    }
  }
  invokeAll(expired);
  return expired.size();
}

UcxTaskLifecycleRegistry::Stats UcxTaskLifecycleRegistry::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return Stats{
      expectedTasks_.size(),
      pendingRequestCount_,
      options_.expectedTaskCapacity,
      options_.pendingRequestCapacity,
      options_.maxDestinationsPerTask,
      totalExpected_,
      totalRetired_,
      totalDeferred_,
      totalAdopted_,
      totalExpired_,
      totalRejected_};
}

void UcxTaskLifecycleRegistry::shutdown() noexcept {
  std::vector<ResolvedRequest> expired;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
    for (auto& [taskId, requests] : pending_) {
      for (auto& request : requests) {
        expired.push_back(
            ResolvedRequest{
                std::move(request.onExpected),
                std::move(request.onRejected),
                AdmissionRejectReason::kShutdown,
                false});
      }
    }
    totalExpired_ += pendingRequestCount_;
    pendingRequestCount_ = 0;
    pending_.clear();
    expectedTasks_.clear();
  }
  cv_.notify_all();
  if (reaperThread_.joinable()) {
    reaperThread_.join();
  }
  invokeAll(expired);
}

void UcxTaskLifecycleRegistry::invokeAll(
    std::vector<ResolvedRequest>& callbacks) noexcept {
  for (auto& callback : callbacks) {
    try {
      if (callback.accepted) {
        callback.onExpected();
      } else {
        callback.onRejected(callback.reason);
      }
    } catch (...) {
      // Lifecycle cleanup is fail-closed and must continue invoking peers.
    }
  }
}

void UcxTaskLifecycleRegistry::reaperLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_) {
    if (pendingRequestCount_ == 0) {
      cv_.wait(
          lock, [this]() { return stopping_ || pendingRequestCount_ > 0; });
      continue;
    }
    auto earliest = Clock::time_point::max();
    for (const auto& [taskId, requests] : pending_) {
      for (const auto& request : requests) {
        earliest = std::min(earliest, request.deadline);
      }
    }
    cv_.wait_until(lock, earliest);
    if (stopping_) {
      break;
    }
    lock.unlock();
    reapExpired();
    lock.lock();
  }
}

} // namespace facebook::velox::ucx_exchange
