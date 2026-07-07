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
#include "velox/experimental/ucx-exchange/UcxExchangeProtocol.h"

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
      "VELOX_UCX_MAX_PENDING_UNKNOWN_REQUESTS", options.pendingRequestCapacity);
  options.activeHandshakeCapacity = positiveEnvironmentValue(
      "VELOX_UCX_MAX_ACTIVE_HANDSHAKES", options.activeHandshakeCapacity);
  options.activeHandshakesPerTaskCapacity = positiveEnvironmentValue(
      "VELOX_UCX_MAX_ACTIVE_HANDSHAKES_PER_TASK",
      options.activeHandshakesPerTaskCapacity);
  const auto maxDestinations = positiveEnvironmentValue(
      "VELOX_UCX_MAX_DESTINATIONS_PER_TASK", options.maxDestinationsPerTask);
  if (maxDestinations <= std::numeric_limits<uint32_t>::max()) {
    options.maxDestinationsPerTask = static_cast<uint32_t>(maxDestinations);
  }
  const auto unknownWaitMs = positiveEnvironmentValue(
      "VELOX_UCX_UNKNOWN_TASK_WAIT_MS",
      static_cast<size_t>(options.unknownTaskWait.count()));
  if (unknownWaitMs <=
      static_cast<size_t>(
          std::numeric_limits<std::chrono::milliseconds::rep>::max())) {
    options.unknownTaskWait = std::chrono::milliseconds(unknownWaitMs);
  }
  return options;
}

UcxTaskLifecycleRegistry::OutputKind lifecycleOutputKind(
    core::PartitionedOutputNode::Kind kind) {
  switch (kind) {
    case core::PartitionedOutputNode::Kind::kPartitioned:
      return UcxTaskLifecycleRegistry::OutputKind::kPartitioned;
    case core::PartitionedOutputNode::Kind::kBroadcast:
      return UcxTaskLifecycleRegistry::OutputKind::kBroadcast;
    case core::PartitionedOutputNode::Kind::kArbitrary:
      return UcxTaskLifecycleRegistry::OutputKind::kArbitrary;
  }
  VELOX_UNREACHABLE("Unsupported UCX output kind");
}

core::PartitionedOutputNode::Kind veloxOutputKind(
    UcxTaskLifecycleRegistry::OutputKind kind) {
  switch (kind) {
    case UcxTaskLifecycleRegistry::OutputKind::kPartitioned:
      return core::PartitionedOutputNode::Kind::kPartitioned;
    case UcxTaskLifecycleRegistry::OutputKind::kBroadcast:
      return core::PartitionedOutputNode::Kind::kBroadcast;
    case UcxTaskLifecycleRegistry::OutputKind::kArbitrary:
      return core::PartitionedOutputNode::Kind::kArbitrary;
  }
  VELOX_UNREACHABLE("Unsupported UCX lifecycle output kind");
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
    : taskLifecycle_(options),
      activeHandshakeCapacity_(options.activeHandshakeCapacity),
      activeHandshakesPerTaskCapacity_(
          options.activeHandshakesPerTaskCapacity) {
  VELOX_CHECK_GT(
      activeHandshakeCapacity_,
      0,
      "UCX active handshake capacity must be positive");
  VELOX_CHECK_GT(
      activeHandshakesPerTaskCapacity_,
      0,
      "UCX per-task active handshake capacity must be positive");
}

UcxOutputQueueManager::~UcxOutputQueueManager() {
  taskLifecycle_.shutdown();
  // Do not call the process-wide intra-node singleton here: function-local
  // singleton destruction order across translation units is unspecified. Live
  // tasks are cancelled by removeTask(); process teardown only drops local
  // bookkeeping.
  std::lock_guard<std::mutex> lock(handshakeMutex_);
  activeHandshakes_.clear();
  activeHandshakesPerTask_.clear();
  intraNodeTaskTokens_.clear();
}

UcxOutputQueueManager::HandshakeReservation::HandshakeReservation(
    std::weak_ptr<UcxOutputQueueManager> owner,
    TaskToken taskToken,
    uint32_t destination,
    uintptr_t endpointIdentity,
    uint64_t reservationId)
    : owner_(std::move(owner)),
      taskToken_(std::move(taskToken)),
      destination_(destination),
      endpointIdentity_(endpointIdentity),
      reservationId_(reservationId) {}

UcxOutputQueueManager::HandshakeReservation::~HandshakeReservation() {
  if (auto owner = owner_.lock()) {
    owner->releaseHandshake(
        taskToken_.taskId, destination_, endpointIdentity_, reservationId_);
  }
}

size_t UcxOutputQueueManager::HandshakeReservationKeyHash::operator()(
    const HandshakeReservationKey& key) const {
  size_t hash = std::hash<std::string>{}(key.taskId);
  hash ^= std::hash<uint32_t>{}(key.destination) + 0x9e3779b9 + (hash << 6) +
      (hash >> 2);
  hash ^= std::hash<uintptr_t>{}(key.endpointIdentity) + 0x9e3779b9 +
      (hash << 6) + (hash >> 2);
  return hash;
}

void UcxOutputQueueManager::expectTask(
    std::string_view taskId,
    uint32_t destinationCount,
    core::PartitionedOutputNode::Kind kind) {
  VELOX_CHECK(
      isValidHandshakeTaskId(taskId),
      "UCX task ID is invalid for the handshake protocol: {} bytes",
      taskId.size());
  std::lock_guard<std::mutex> incarnationLock(taskIncarnationMutex_);
  auto intraNodeRegistry = IntraNodeTransferRegistry::getInstance();
  const auto registration = intraNodeRegistry->expectTask(taskId);
  try {
    {
      std::lock_guard<std::mutex> lock(handshakeMutex_);
      const auto [token, inserted] = intraNodeTaskTokens_.emplace(
          std::string{taskId}, registration.token);
      VELOX_CHECK(
          inserted || token->second == registration.token,
          "UCX task {} has conflicting active epochs",
          taskId);
    }
    taskLifecycle_.expectTask(
        taskId,
        UcxTaskLifecycleRegistry::TaskContract{
            destinationCount, lifecycleOutputKind(kind)});
  } catch (...) {
    if (registration.inserted) {
      taskLifecycle_.retireTask(taskId);
      {
        std::lock_guard<std::mutex> lock(handshakeMutex_);
        releaseTaskHandshakesLocked(taskId);
        auto token = intraNodeTaskTokens_.find(std::string{taskId});
        if (token != intraNodeTaskTokens_.end() &&
            token->second == registration.token) {
          intraNodeTaskTokens_.erase(token);
        }
      }
      intraNodeRegistry->cancelTask(registration.token);
    }
    throw;
  }
}

UcxOutputQueueManager::HandshakeAdmissionResult
UcxOutputQueueManager::admitHandshake(
    std::string_view taskId,
    uint32_t destination,
    UcxTaskLifecycleRegistry::Callback onAccepted,
    UcxTaskLifecycleRegistry::RejectCallback onRejected) {
  return taskLifecycle_.admitRequest(
      taskId, destination, std::move(onAccepted), std::move(onRejected));
}

UcxOutputQueueManager::HandshakeReservationResult
UcxOutputQueueManager::reserveHandshake(
    std::string_view taskId,
    uint32_t destination,
    uintptr_t endpointIdentity) {
  VELOX_CHECK(!taskId.empty(), "UCX handshake task ID must not be empty");
  VELOX_CHECK_NE(
      endpointIdentity, 0, "UCX handshake endpoint identity must not be null");
  auto owner = weak_from_this();
  VELOX_CHECK(
      !owner.expired(),
      "UCX handshake reservations require a shared queue manager");
  const std::string taskIdString{taskId};
  HandshakeReservationKey key{taskIdString, destination, endpointIdentity};
  uint64_t reservationId;
  UcxTaskLifecycleRegistry::TaskContract contract;
  TaskToken taskToken;
  {
    std::lock_guard<std::mutex> lock(handshakeMutex_);
    // Serialize this final contract check with removeTask()'s reservation
    // drain. Reading the contract before taking handshakeMutex_ would allow a
    // retire/check/insert race to leave a reservation behind for a dead task.
    const auto currentContract = taskLifecycle_.taskContract(taskId);
    if (!currentContract.has_value()) {
      return {nullptr, HandshakeRejectReason::kRetired, {}};
    }
    contract = *currentContract;
    const auto tokenIt = intraNodeTaskTokens_.find(taskIdString);
    if (tokenIt == intraNodeTaskTokens_.end()) {
      return {nullptr, HandshakeRejectReason::kRetired, contract};
    }
    taskToken = tokenIt->second;
    if (destination >= contract.destinationCount) {
      return {nullptr, HandshakeRejectReason::kInvalidDestination, contract};
    }
    if (activeHandshakes_.count(key) != 0) {
      ++totalDuplicateHandshakes_;
      ++totalHandshakeReservationRejected_;
      return {nullptr, HandshakeRejectReason::kDuplicate, contract};
    }
    auto taskIt = activeHandshakesPerTask_.find(taskIdString);
    const size_t taskCount =
        taskIt == activeHandshakesPerTask_.end() ? 0 : taskIt->second;
    const size_t taskCapacity = std::min<size_t>(
        activeHandshakesPerTaskCapacity_, contract.destinationCount);
    if (activeHandshakes_.size() >= activeHandshakeCapacity_ ||
        taskCount >= taskCapacity) {
      ++totalHandshakeReservationRejected_;
      return {nullptr, HandshakeRejectReason::kCapacity, contract};
    }
    // Zero is reserved as an invalid incarnation. Exhaustion is practically
    // unreachable, but fail closed instead of allowing an ABA collision.
    VELOX_CHECK_NE(
        nextHandshakeReservationId_,
        std::numeric_limits<uint64_t>::max(),
        "UCX handshake reservation incarnation space is exhausted");
    reservationId = nextHandshakeReservationId_++;
    bool insertedTaskCounter = false;
    try {
      if (taskIt == activeHandshakesPerTask_.end()) {
        auto inserted = activeHandshakesPerTask_.emplace(taskIdString, 0);
        taskIt = inserted.first;
        insertedTaskCounter = inserted.second;
      }
      const auto insertedReservation =
          activeHandshakes_.emplace(key, reservationId);
      if (!insertedReservation.second) {
        if (insertedTaskCounter) {
          activeHandshakesPerTask_.erase(taskIt);
        }
        ++totalDuplicateHandshakes_;
        ++totalHandshakeReservationRejected_;
        return {nullptr, HandshakeRejectReason::kDuplicate, contract};
      }
    } catch (...) {
      if (insertedTaskCounter) {
        activeHandshakesPerTask_.erase(taskIt);
      }
      ++totalHandshakeReservationRejected_;
      return {nullptr, HandshakeRejectReason::kCapacity, contract};
    }
    ++taskIt->second;
    ++totalHandshakeReservations_;
  }
  try {
    return {
        std::shared_ptr<HandshakeReservation>(new HandshakeReservation(
            std::move(owner),
            std::move(taskToken),
            destination,
            endpointIdentity,
            reservationId)),
        HandshakeRejectReason::kNone,
        contract};
  } catch (...) {
    // HandshakeReservation allocation happens outside handshakeMutex_. Roll
    // back the already-published map entry without a recursive lock.
    releaseHandshake(
        taskIdString, destination, endpointIdentity, reservationId);
    std::lock_guard<std::mutex> lock(handshakeMutex_);
    ++totalHandshakeReservationRejected_;
    return {nullptr, HandshakeRejectReason::kCapacity, contract};
  }
}

std::optional<UcxTaskLifecycleRegistry::TaskContract>
UcxOutputQueueManager::taskContract(std::string_view taskId) const {
  return taskLifecycle_.taskContract(taskId);
}

std::optional<TaskToken> UcxOutputQueueManager::taskToken(
    std::string_view taskId) const {
  std::lock_guard<std::mutex> lock(handshakeMutex_);
  auto token = intraNodeTaskTokens_.find(std::string{taskId});
  if (token == intraNodeTaskTokens_.end()) {
    return std::nullopt;
  }
  return token->second;
}

void UcxOutputQueueManager::initializeTask(
    std::shared_ptr<exec::Task> task,
    core::PartitionedOutputNode::Kind kind,
    int numDestinations,
    int numDrivers) {
  totalInitializeCalls_.fetch_add(1, std::memory_order_relaxed);
  const auto& taskId = task->taskId();
  VELOX_CHECK_GT(numDestinations, 0, "UCX task must have a destination");
  const auto actualKind = lifecycleOutputKind(kind);
  auto contract = taskLifecycle_.taskContract(taskId);
  if (!contract.has_value()) {
    expectTask(taskId, static_cast<uint32_t>(numDestinations), kind);
    contract = taskLifecycle_.taskContract(taskId);
  }
  VELOX_CHECK(
      contract.has_value(),
      "UCX task contract disappeared during initialization");
  VELOX_CHECK_EQ(
      static_cast<uint32_t>(contract->outputKind),
      static_cast<uint32_t>(actualKind),
      "UCX task {} output kind differs from its admitted contract",
      taskId);
  if (kind == core::PartitionedOutputNode::Kind::kBroadcast) {
    VELOX_CHECK_LE(
        static_cast<uint32_t>(numDestinations),
        contract->destinationCount,
        "UCX broadcast task {} initializes more destinations than admitted",
        taskId);
  } else {
    VELOX_CHECK_EQ(
        static_cast<uint32_t>(numDestinations),
        contract->destinationCount,
        "UCX task {} destination count differs from its admitted contract",
        taskId);
  }
  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskId);
    if (it == queues.end()) {
      queues[taskId] = std::make_shared<UcxOutputQueue>(
          std::move(task),
          numDestinations,
          numDrivers,
          kind,
          contract->destinationCount);
    } else {
      if (!it->second->initialize(
              task,
              numDestinations,
              numDrivers,
              kind,
              contract->destinationCount)) {
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
  VELOX_CHECK_GT(numBuffers, 0, "UCX output buffer count must be positive");
  const auto contract =
      reconcileOutputBufferContract(taskId, static_cast<uint32_t>(numBuffers));
  auto queue = getQueue(taskId);
  if (contract.outputKind == UcxTaskLifecycleRegistry::OutputKind::kBroadcast) {
    queue->expandBroadcastDestinationLimit(contract.destinationCount);
  }
  queue->updateOutputBuffers(numBuffers, noMoreBuffers);
}

bool UcxOutputQueueManager::updateOutputBuffersIfExists(
    std::string_view taskId,
    int numBuffers,
    bool noMoreBuffers) {
  if (auto queue = getQueueIfExists(taskId)) {
    VELOX_CHECK_GT(numBuffers, 0, "UCX output buffer count must be positive");
    const auto contract = reconcileOutputBufferContract(
        taskId, static_cast<uint32_t>(numBuffers));
    if (contract.outputKind ==
        UcxTaskLifecycleRegistry::OutputKind::kBroadcast) {
      queue->expandBroadcastDestinationLimit(contract.destinationCount);
    }
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
  VELOX_CHECK_GE(destination, 0, "UCX destination must not be negative");
  std::shared_ptr<UcxOutputQueue> outputQueue;
  std::string taskIdStr{taskId};
  auto pendingNotify =
      std::make_shared<UcxDataAvailableCallback>(std::move(notify));
  auto reject =
      [pendingNotify](UcxTaskLifecycleRegistry::AdmissionRejectReason) mutable {
        if (*pendingNotify) {
          (*pendingNotify)(nullptr, {});
        }
      };
  const auto admission = admitHandshake(
      taskIdStr,
      static_cast<uint32_t>(destination),
      [this, taskIdStr, destination, pendingNotify]() mutable {
        getData(taskIdStr, destination, std::move(*pendingNotify));
      },
      reject);
  if (admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred) {
    return;
  }
  if (admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected) {
    reject(admission.rejectReason);
    return;
  }

  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    if (it == queues.end()) {
      // create the queue structures such that the notify callback can be
      // stored. It will be later initialized once the task is being created.
      VLOG(2)
          << "[QUEUE-MGR] task=" << taskId << " dest=" << destination
          << " creating placeholder queue (server arrived before task init)";
      outputQueue = std::make_shared<UcxOutputQueue>(
          nullptr,
          static_cast<uint32_t>(destination) + 1,
          0,
          veloxOutputKind(admission.contract.outputKind),
          admission.contract.destinationCount);
      queues[taskIdStr] = outputQueue;
    } else {
      // queue exists.
      outputQueue = it->second;
    }
  });
  if (admission.contract.outputKind ==
      UcxTaskLifecycleRegistry::OutputKind::kBroadcast) {
    outputQueue->expandBroadcastDestinationLimit(
        admission.contract.destinationCount);
  }
  // outside of lock. Queue must exist.
  // get the data or install the notify callback.
  outputQueue->getData(destination, std::move(*pendingNotify));
}

void UcxOutputQueueManager::getData(
    std::string_view taskId,
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    UcxDataAvailableCallbackV2 notify) {
  VELOX_CHECK_GE(destination, 0, "UCX destination must not be negative");
  std::shared_ptr<UcxOutputQueue> outputQueue;
  std::string taskIdStr{taskId};
  auto pendingNotify =
      std::make_shared<UcxDataAvailableCallbackV2>(std::move(notify));
  auto reject = [sequence, pendingNotify](
                    UcxTaskLifecycleRegistry::AdmissionRejectReason) mutable {
    if (*pendingNotify) {
      (*pendingNotify)(nullptr, sequence, {});
    }
  };
  const auto admission = admitHandshake(
      taskIdStr,
      static_cast<uint32_t>(destination),
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
      reject);
  if (admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred) {
    return;
  }
  if (admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected) {
    reject(admission.rejectReason);
    return;
  }

  queues_.withLock([&](auto& queues) {
    auto it = queues.find(taskIdStr);
    if (it == queues.end()) {
      VLOG(2)
          << "[QUEUE-MGR] task=" << taskId << " dest=" << destination
          << " creating placeholder queue (server arrived before task init)";
      outputQueue = std::make_shared<UcxOutputQueue>(
          nullptr,
          static_cast<uint32_t>(destination) + 1,
          0,
          veloxOutputKind(admission.contract.outputKind),
          admission.contract.destinationCount);
      queues[taskIdStr] = outputQueue;
    } else {
      outputQueue = it->second;
    }
  });
  if (admission.contract.outputKind ==
      UcxTaskLifecycleRegistry::OutputKind::kBroadcast) {
    outputQueue->expandBroadcastDestinationLimit(
        admission.contract.destinationCount);
  }
  outputQueue->getData(
      destination, maxBytes, sequence, std::move(*pendingNotify));
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
  bool expected{false};
  std::optional<TaskToken> taskToken;
  {
    std::lock_guard<std::mutex> incarnationLock(taskIncarnationMutex_);
    expected = taskLifecycle_.retireTask(taskIdStr);
    {
      std::lock_guard<std::mutex> lock(handshakeMutex_);
      releaseTaskHandshakesLocked(taskIdStr);
      auto token = intraNodeTaskTokens_.find(taskIdStr);
      if (token != intraNodeTaskTokens_.end()) {
        taskToken = token->second;
        intraNodeTaskTokens_.erase(token);
      }
    }
    if (taskToken.has_value()) {
      IntraNodeTransferRegistry::getInstance()->cancelTask(*taskToken);
    }
  }
  // Retire intra-node state before erasing the queue. Pollers observe explicit
  // CANCELLED (never successful EOS), and every producer future is fulfilled.
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
          << " queueExists=" << (queue != nullptr) << " expected=" << expected;
  if (queue != nullptr) {
    totalQueuesRemoved_.fetch_add(1, std::memory_order_relaxed);
    queue->terminate();
  }
}

UcxOutputQueueManager::RegistryStats UcxOutputQueueManager::registryStats()
    const {
  RegistryStats result;
  result.activeQueues =
      queues_.withLock([](const auto& queues) { return queues.size(); });
  const auto lifecycle = taskLifecycle_.stats();
  result.activeExpectedTasks = lifecycle.activeExpectedTasks;
  result.pendingUnknownRequests = lifecycle.pendingUnknownRequests;
  result.expectedTaskCapacity = lifecycle.expectedTaskCapacity;
  result.pendingRequestCapacity = lifecycle.pendingRequestCapacity;
  result.maxDestinationsPerTask = lifecycle.maxDestinationsPerTask;
  {
    std::lock_guard<std::mutex> lock(handshakeMutex_);
    result.activeHandshakeReservations = activeHandshakes_.size();
    result.activeHandshakeCapacity = activeHandshakeCapacity_;
    result.activeHandshakesPerTaskCapacity = activeHandshakesPerTaskCapacity_;
    result.totalHandshakeReservations = totalHandshakeReservations_;
    result.totalDuplicateHandshakes = totalDuplicateHandshakes_;
    result.totalHandshakeReservationRejected =
        totalHandshakeReservationRejected_;
  }
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

void UcxOutputQueueManager::releaseHandshake(
    std::string_view taskId,
    uint32_t destination,
    uintptr_t endpointIdentity,
    uint64_t reservationId) noexcept {
  std::lock_guard<std::mutex> lock(handshakeMutex_);
  const HandshakeReservationKey key{
      std::string{taskId}, destination, endpointIdentity};
  const auto reservationIt = activeHandshakes_.find(key);
  if (reservationIt == activeHandshakes_.end() ||
      reservationIt->second != reservationId) {
    return;
  }
  activeHandshakes_.erase(reservationIt);
  auto taskIt = activeHandshakesPerTask_.find(std::string{taskId});
  if (taskIt == activeHandshakesPerTask_.end()) {
    return;
  }
  VELOX_DCHECK_GT(taskIt->second, 0);
  if (--taskIt->second == 0) {
    activeHandshakesPerTask_.erase(taskIt);
  }
}

void UcxOutputQueueManager::releaseTaskHandshakes(
    std::string_view taskId) noexcept {
  std::lock_guard<std::mutex> lock(handshakeMutex_);
  releaseTaskHandshakesLocked(taskId);
}

void UcxOutputQueueManager::releaseTaskHandshakesLocked(
    std::string_view taskId) noexcept {
  const std::string taskIdString{taskId};
  for (auto it = activeHandshakes_.begin(); it != activeHandshakes_.end();) {
    if (it->first.taskId == taskIdString) {
      it = activeHandshakes_.erase(it);
    } else {
      ++it;
    }
  }
  activeHandshakesPerTask_.erase(taskIdString);
}

UcxTaskLifecycleRegistry::TaskContract
UcxOutputQueueManager::reconcileOutputBufferContract(
    std::string_view taskId,
    uint32_t numBuffers) {
  auto contract = taskLifecycle_.taskContract(taskId);
  VELOX_CHECK(contract.has_value(), "UCX task contract not found: {}", taskId);
  if (contract->outputKind ==
      UcxTaskLifecycleRegistry::OutputKind::kBroadcast) {
    // A Spark-side pre-declaration can already contain the final fanout while
    // Velox exposes buffers incrementally. Counts below that admitted upper
    // bound are observations, not contract shrink attempts.
    if (numBuffers > contract->destinationCount) {
      taskLifecycle_.expandBroadcastDestinations(taskId, numBuffers);
      contract = taskLifecycle_.taskContract(taskId);
      VELOX_CHECK(
          contract.has_value(),
          "UCX task contract disappeared while expanding buffers: {}",
          taskId);
    }
    return *contract;
  }
  // For non-broadcast output, this validates that the exact count did not
  // change and throws on mismatch.
  taskLifecycle_.expandBroadcastDestinations(taskId, numBuffers);
  return *contract;
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
