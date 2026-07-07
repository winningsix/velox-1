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
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"
#include <cudf/detail/nvtx/ranges.hpp>
#include <cudf/io/types.hpp>
#include <cudf/table/table.hpp>
#include <fmt/format.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/exec_policy.hpp>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <utility>
#include "velox/experimental/ucx-exchange/IntraNodeTransferRegistry.h"

namespace facebook::velox::ucx_exchange {

namespace {

size_t positiveEnvironmentValue(const char* name, size_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0' || value[0] == '-') {
    return fallback;
  }
  errno = 0;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<size_t>::max()) {
    return fallback;
  }
  return static_cast<size_t>(parsed);
}

UcxTaskLifecycleRegistry::Options processLifecycleOptions() {
  UcxTaskLifecycleRegistry::Options options;
  options.expectedTaskCapacity = positiveEnvironmentValue(
      "VELOX_UCX_MAX_EXPECTED_TASKS", options.expectedTaskCapacity);
  options.pendingRequestCapacity = positiveEnvironmentValue(
      "VELOX_UCX_MAX_PENDING_UNKNOWN_REQUESTS",
      options.pendingRequestCapacity);
  const auto unknownWaitMs = positiveEnvironmentValue(
      "VELOX_UCX_UNKNOWN_TASK_WAIT_MS",
      static_cast<size_t>(options.unknownTaskWait.count()));
  if (unknownWaitMs <= static_cast<size_t>(
                           std::numeric_limits<
                               std::chrono::milliseconds::rep>::max())) {
    options.unknownTaskWait = std::chrono::milliseconds(unknownWaitMs);
  }
  return options;
}

} // namespace

/* static */
std::shared_ptr<UcxOutputQueueManager> UcxOutputQueueManager::getInstanceRef() {
  // In C++11, the static local variable is guaranteed to only be initialized
  // once even in a multi-threaded context.
  static std::shared_ptr<UcxOutputQueueManager> instance =
      create(processLifecycleOptions());
  return instance;
}

std::shared_ptr<UcxOutputQueueManager> UcxOutputQueueManager::create(
    UcxTaskLifecycleRegistry::Options options) {
  return std::make_shared<UcxOutputQueueManager>(std::move(options));
}

UcxOutputQueueManager::UcxOutputQueueManager()
    : UcxOutputQueueManager(UcxTaskLifecycleRegistry::Options{}) {}

UcxOutputQueueManager::UcxOutputQueueManager(
    UcxTaskLifecycleRegistry::Options options)
    : taskLifecycle_(std::move(options)) {}

UcxOutputQueueManager::~UcxOutputQueueManager() {
  taskLifecycle_.shutdown();
}

void UcxOutputQueueManager::expectTask(std::string_view taskId) {
  auto intraNodeRegistry = IntraNodeTransferRegistry::getInstance();
  intraNodeRegistry->expectTask(taskId);
  try {
    taskLifecycle_.expectTask(taskId);
  } catch (...) {
    intraNodeRegistry->cancelTask(taskId);
    throw;
  }
}

void UcxOutputQueueManager::initializeTask(
    std::shared_ptr<exec::Task> task,
    core::PartitionedOutputNode::Kind kind,
    int numDestinations,
    int numDrivers) {
  totalInitializeCalls_.fetch_add(1, std::memory_order_relaxed);
  const auto& taskId = task->taskId();
  expectTask(taskId);
  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskId);
    if (it == queues.end()) {
      queues[taskId] = std::make_shared<UcxOutputQueue>(
          std::move(task), numDestinations, numDrivers, kind);
    } else {
      if (!it->second->initialize(task, numDestinations, numDrivers, kind)) {
        VELOX_CHECK(
            it->second->isInitialized(),
            "Registering a cudf output queue for pre-existing uninitialized taskId {}",
            taskId);
        VLOG(2) << "[QUEUE-MGR] task=" << taskId
                << " initializeTask ignored (already initialized)";
      }
    }
  });
}

void UcxOutputQueueManager::updateOutputBuffers(
    std::string_view taskId,
    int numBuffers,
    bool noMoreBuffers) {
  getQueue(taskId)->updateOutputBuffers(numBuffers, noMoreBuffers);
}

bool UcxOutputQueueManager::updateOutputBuffersIfExists(
    std::string_view taskId,
    int numBuffers,
    bool noMoreBuffers) {
  if (auto queue = getQueueIfExists(taskId)) {
    queue->updateOutputBuffers(numBuffers, noMoreBuffers);
    return true;
  }
  return false;
}

void UcxOutputQueueManager::enqueue(
    std::string_view taskId,
    int destination,
    std::unique_ptr<cudf::packed_columns> txData,
    int numRows) {
  getQueue(taskId)->enqueue(destination, std::move(txData), numRows);
}

bool UcxOutputQueueManager::checkBlocked(
    std::string_view taskId,
    ContinueFuture* future) {
  return getQueue(taskId)->checkBlocked(future);
}

void UcxOutputQueueManager::noMoreData(std::string_view taskId) {
  getQueue(taskId)->noMoreData();
}

bool UcxOutputQueueManager::isFinished(std::string_view taskId) {
  return getQueue(taskId)->isFinished();
}

void UcxOutputQueueManager::deleteResults(
    std::string_view taskId,
    int destination) {
  if (auto queue = getQueueIfExists(taskId)) {
    queue->deleteResults(destination);
  }
}

void UcxOutputQueueManager::getData(
    std::string_view taskId,
    int destination,
    UcxDataAvailableCallback notify) {
  std::shared_ptr<UcxOutputQueue> outputQueue;
  bool requestDeferred = false;
  bool requestRejected = false;
  std::shared_ptr<UcxDataAvailableCallback> pendingNotify;
  std::string taskIdStr{taskId};
  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    if (it == queues.end()) {
      pendingNotify =
          std::make_shared<UcxDataAvailableCallback>(std::move(notify));
      const auto disposition = taskLifecycle_.deferIfUnexpected(
          taskIdStr,
          [this, taskIdStr, destination, pendingNotify]() mutable {
            getData(taskIdStr, destination, std::move(*pendingNotify));
          },
          [pendingNotify]() mutable {
            if (*pendingNotify) {
              (*pendingNotify)(nullptr, {});
            }
          });
      if (disposition ==
          UcxTaskLifecycleRegistry::RequestDisposition::kDeferred) {
        requestDeferred = true;
        return;
      }
      if (disposition ==
          UcxTaskLifecycleRegistry::RequestDisposition::kRejected) {
        requestRejected = true;
        return;
      }
      // create the queue structures such that the notify callback can be
      // stored. It will be later initialized once the task is being created.
      VLOG(2)
          << "[QUEUE-MGR] task=" << taskId << " dest=" << destination
          << " creating placeholder queue (server arrived before task init)";
      outputQueue = std::make_shared<UcxOutputQueue>(nullptr, destination, 0);
      queues[taskIdStr] = outputQueue;
    } else {
      // queue exists.
      outputQueue = it->second;
    }
  });
  if (requestDeferred) {
    return;
  }
  if (requestRejected) {
    if (*pendingNotify) {
      (*pendingNotify)(nullptr, {});
    }
    return;
  }
  // outside of lock. Queue must exist.
  // get the data or install the notify callback.
  outputQueue->getData(
      destination,
      pendingNotify ? std::move(*pendingNotify) : std::move(notify));
}

void UcxOutputQueueManager::getData(
    std::string_view taskId,
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    UcxDataAvailableCallbackV2 notify) {
  std::shared_ptr<UcxOutputQueue> outputQueue;
  bool requestDeferred = false;
  bool requestRejected = false;
  std::shared_ptr<UcxDataAvailableCallbackV2> pendingNotify;
  std::string taskIdStr{taskId};
  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    if (it == queues.end()) {
      pendingNotify =
          std::make_shared<UcxDataAvailableCallbackV2>(std::move(notify));
      const auto disposition = taskLifecycle_.deferIfUnexpected(
          taskIdStr,
          [this,
           taskIdStr,
           destination,
           maxBytes,
           sequence,
           pendingNotify]() mutable {
            getData(
                taskIdStr,
                destination,
                maxBytes,
                sequence,
                std::move(*pendingNotify));
          },
          [sequence, pendingNotify]() mutable {
            if (*pendingNotify) {
              (*pendingNotify)(nullptr, sequence, {});
            }
          });
      if (disposition ==
          UcxTaskLifecycleRegistry::RequestDisposition::kDeferred) {
        requestDeferred = true;
        return;
      }
      if (disposition ==
          UcxTaskLifecycleRegistry::RequestDisposition::kRejected) {
        requestRejected = true;
        return;
      }
      VLOG(2)
          << "[QUEUE-MGR] task=" << taskId << " dest=" << destination
          << " creating placeholder queue (server arrived before task init)";
      outputQueue = std::make_shared<UcxOutputQueue>(nullptr, destination, 0);
      queues[taskIdStr] = outputQueue;
    } else {
      outputQueue = it->second;
    }
  });
  if (requestDeferred) {
    return;
  }
  if (requestRejected) {
    if (*pendingNotify) {
      (*pendingNotify)(nullptr, sequence, {});
    }
    return;
  }
  outputQueue->getData(
      destination,
      maxBytes,
      sequence,
      pendingNotify ? std::move(*pendingNotify) : std::move(notify));
}

bool UcxOutputQueueManager::canUseIntraNode(std::string_view taskId) {
  auto queue = getQueueIfExists(taskId);
  if (!queue) {
    return false;
  }
  if (!queue->isInitialized()) {
    return false;
  }
  return true;
}

std::string UcxOutputQueueManager::describeQueueForIntraNode(
    std::string_view taskId) {
  auto queue = getQueueIfExists(taskId);
  if (!queue) {
    return "missing";
  }
  return fmt::format(
      "initialized={} kind={}",
      queue->isInitialized(),
      core::PartitionedOutputNode::toName(queue->kind()));
}

void UcxOutputQueueManager::removeTask(std::string_view taskId) {
  totalRemoveCalls_.fetch_add(1, std::memory_order_relaxed);
  std::string taskIdStr{taskId};
  const auto expected = taskLifecycle_.retireTask(taskIdStr);
  // Retire intra-node state before erasing the queue. Pollers become atEnd and
  // every outstanding entry is fulfilled; no historical task ID is retained.
  IntraNodeTransferRegistry::getInstance()->cancelTask(taskIdStr);
  auto queue =
      queues_.withLock([&](auto& queues) -> std::shared_ptr<UcxOutputQueue> {
        auto it = queues.find(taskIdStr);
        if (it == queues.end()) {
          return nullptr;
        }
        auto taskQueue = it->second;
        queues.erase(it);
        return taskQueue;
      });
  VLOG(2) << "[QUEUE-MGR] removeTask=" << taskId
          << " queueExists=" << (queue != nullptr)
          << " expected=" << expected;
  if (queue != nullptr) {
    totalQueuesRemoved_.fetch_add(1, std::memory_order_relaxed);
    queue->terminate();
  }
}

UcxOutputQueueManager::RegistryStats
UcxOutputQueueManager::registryStats() const {
  RegistryStats result;
  result.activeQueues =
      queues_.withLock([](const auto& queues) { return queues.size(); });
  const auto lifecycle = taskLifecycle_.stats();
  result.activeExpectedTasks = lifecycle.activeExpectedTasks;
  result.pendingUnknownRequests = lifecycle.pendingUnknownRequests;
  result.expectedTaskCapacity = lifecycle.expectedTaskCapacity;
  result.pendingRequestCapacity = lifecycle.pendingRequestCapacity;
  result.totalInitializeCalls =
      totalInitializeCalls_.load(std::memory_order_relaxed);
  result.totalRemoveCalls = totalRemoveCalls_.load(std::memory_order_relaxed);
  result.totalQueuesRemoved =
      totalQueuesRemoved_.load(std::memory_order_relaxed);
  result.totalExpected = lifecycle.totalExpected;
  result.totalRetired = lifecycle.totalRetired;
  result.totalDeferred = lifecycle.totalDeferred;
  result.totalAdopted = lifecycle.totalAdopted;
  result.totalExpired = lifecycle.totalExpired;
  result.totalRejected = lifecycle.totalRejected;
  return result;
}

std::shared_ptr<UcxOutputQueue> UcxOutputQueueManager::getQueueIfExists(
    std::string_view taskId) {
  std::string taskIdStr{taskId};
  return queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    return it == queues.end() ? nullptr : it->second;
  });
}

std::shared_ptr<UcxOutputQueue> UcxOutputQueueManager::getQueue(
    std::string_view taskId) {
  std::string taskIdStr{taskId};
  return queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    VELOX_CHECK(
        it != queues.end(), "Output cudf queue for task not found: {}", taskId);
    return it->second;
  });
}

std::optional<exec::OutputBuffer::Stats> UcxOutputQueueManager::stats(
    std::string_view taskId) {
  auto queue = getQueueIfExists(taskId);
  if (queue != nullptr) {
    return queue->stats();
  }
  return std::nullopt;
}

} // namespace facebook::velox::ucx_exchange
