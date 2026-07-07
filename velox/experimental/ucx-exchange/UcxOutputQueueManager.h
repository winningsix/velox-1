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
#include <velox/exec/Task.h>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include "velox/experimental/ucx-exchange/UcxQueues.h"
#include "velox/experimental/ucx-exchange/UcxTaskLifecycleRegistry.h"
#include "velox/experimental/ucx-exchange/UcxTaskToken.h"

namespace facebook::velox::ucx_exchange {

class UcxOutputQueueManager
    : public std::enable_shared_from_this<UcxOutputQueueManager> {
 public:
  struct RegistryStats {
    size_t activeQueues{0};
    size_t activeExpectedTasks{0};
    size_t pendingUnknownRequests{0};
    size_t expectedTaskCapacity{0};
    size_t pendingRequestCapacity{0};
    uint32_t maxDestinationsPerTask{0};
    size_t activeHandshakeReservations{0};
    size_t activeHandshakeCapacity{0};
    size_t activeHandshakesPerTaskCapacity{0};
    uint64_t totalInitializeCalls{0};
    uint64_t totalRemoveCalls{0};
    uint64_t totalQueuesRemoved{0};
    uint64_t totalExpected{0};
    uint64_t totalRetired{0};
    uint64_t totalDeferred{0};
    uint64_t totalAdopted{0};
    uint64_t totalExpired{0};
    uint64_t totalRejected{0};
    uint64_t totalHandshakeReservations{0};
    uint64_t totalDuplicateHandshakes{0};
    uint64_t totalHandshakeReservationRejected{0};
  };

  /// A live reservation for one admitted (task, destination, endpoint)
  /// exchange server. The last owner releases it exactly once.
  class HandshakeReservation {
   public:
    HandshakeReservation(const HandshakeReservation&) = delete;
    HandshakeReservation& operator=(const HandshakeReservation&) = delete;
    ~HandshakeReservation();

    const TaskToken& taskToken() const {
      return taskToken_;
    }

   private:
    friend class UcxOutputQueueManager;
    HandshakeReservation(
        std::weak_ptr<UcxOutputQueueManager> owner,
        TaskToken taskToken,
        uint32_t destination,
        uintptr_t endpointIdentity,
        uint64_t reservationId);

    std::weak_ptr<UcxOutputQueueManager> owner_;
    const TaskToken taskToken_;
    const uint32_t destination_;
    const uintptr_t endpointIdentity_;
    // A manager-local reservation incarnation, independent of the task epoch.
    // Both must match before a release may erase live state.
    const uint64_t reservationId_;
  };

  struct HandshakeReservationResult {
    std::shared_ptr<HandshakeReservation> reservation;
    UcxTaskLifecycleRegistry::AdmissionRejectReason rejectReason{
        UcxTaskLifecycleRegistry::AdmissionRejectReason::kNone};
    UcxTaskLifecycleRegistry::TaskContract contract;

    explicit operator bool() const {
      return reservation != nullptr;
    }
  };

  /// Factory method to retrieve a reference to the output queue manager.
  static std::shared_ptr<UcxOutputQueueManager> getInstanceRef();

  /// Creates an isolated manager with explicit lifecycle bounds. Primarily
  /// used by concurrency tests and embedders that do not use the singleton.
  static std::shared_ptr<UcxOutputQueueManager> create(
      UcxTaskLifecycleRegistry::Options options);

  UcxOutputQueueManager();
  explicit UcxOutputQueueManager(UcxTaskLifecycleRegistry::Options options);
  ~UcxOutputQueueManager();
  // no copy constructor.
  UcxOutputQueueManager(const UcxOutputQueueManager&) = delete;
  // no copy assignment.
  UcxOutputQueueManager& operator=(const UcxOutputQueueManager&) = delete;

  /// Declares an exact task output contract before initializeTask(). Early
  /// handshakes for undeclared tasks are held only within the lifecycle
  /// registry's hard cap/deadline.
  void expectTask(
      std::string_view taskId,
      uint32_t destinationCount,
      core::PartitionedOutputNode::Kind kind);

  using HandshakeAdmissionResult = UcxTaskLifecycleRegistry::AdmissionResult;
  using HandshakeRejectReason = UcxTaskLifecycleRegistry::AdmissionRejectReason;

  /// Performs task and destination admission before Acceptor creates an
  /// exchange server or acknowledges a handshake.
  HandshakeAdmissionResult admitHandshake(
      std::string_view taskId,
      uint32_t destination,
      UcxTaskLifecycleRegistry::Callback onAccepted,
      UcxTaskLifecycleRegistry::RejectCallback onRejected);

  /// Reserves bounded server state after task admission and before any
  /// endpoint element or communicator work item is created. Duplicate keys
  /// and global/per-task capacity exhaustion fail closed.
  HandshakeReservationResult reserveHandshake(
      std::string_view taskId,
      uint32_t destination,
      uintptr_t endpointIdentity);

  std::optional<UcxTaskLifecycleRegistry::TaskContract> taskContract(
      std::string_view taskId) const;

  std::optional<TaskToken> taskToken(std::string_view taskId) const;

  /// @brief Initializes a task and creates the corresponding output queues that
  /// are associated with this task.
  /// @param task The task.
  /// @param kind The output mode (partitioned, broadcast, etc.)
  /// @param numDestinations The number of queues (destinations or partitions)
  /// associated with this task.
  /// @param numDrivers The number of drivers that contribute data to these
  /// queues. Used to recognize when the queues are complete.
  void initializeTask(
      std::shared_ptr<exec::Task> task,
      core::PartitionedOutputNode::Kind kind,
      int numDestinations,
      int numDrivers);

  /// @brief Updates the number of destination buffers for a task.
  /// For broadcast mode, new destinations are backfilled with previously
  /// broadcast data.
  void updateOutputBuffers(
      std::string_view taskId,
      int numBuffers,
      bool noMoreBuffers);

  /// Same as updateOutputBuffers(), but returns false instead of failing when
  /// the UCX output queue has not been created for this task.
  bool updateOutputBuffersIfExists(
      std::string_view taskId,
      int numBuffers,
      bool noMoreBuffers);

  /// @brief Enqueues a cudf packed column into the queue.
  /// @param taskId The unique task Id.
  /// @param destination The destination (partition, queue number) into which
  /// the data is queued.
  /// @param txData The data to enqueue.
  /// @param numRows The number of rows in the data.
  void enqueue(
      std::string_view taskId,
      int destination,
      std::unique_ptr<cudf::packed_columns> txData,
      int32_t numRows);

  /// @brief Checks if the queue for a task is over capacity.
  /// Should be called after enqueueing all partitions for a batch.
  /// @param taskId The unique task Id.
  /// @param future Output parameter - populated with a future if blocked.
  /// @return True if blocked (queue over capacity), false otherwise.
  bool checkBlocked(std::string_view taskId, ContinueFuture* future);

  /// @brief Indicates that no more data will be coming for this task.
  void noMoreData(std::string_view taskId);

  /// @returns true if noMoreData has been called and all the accumulated data
  /// have been fetched and acknowledged.
  bool isFinished(std::string_view taskId);

  /// @brief
  void deleteResults(std::string_view taskId, int destination);

  /// @brief Asynchronously returns the head of the queue. If data is available,
  /// the callback function is triggered immediately and true is returned.
  /// Otherwise, the callback function is registered and called once data is
  /// available. If the queue is closed and no more data is available, then the
  /// callback function is called immediately with a nullptr. If the destination
  /// doesn't exist, then additional queues will be added for this task.
  /// @param taskId The unique taskId.
  /// @param destination The destination.
  /// @param notify The callback function.
  void getData(
      std::string_view taskId,
      int destination,
      UcxDataAvailableCallback notify);

  void getData(
      std::string_view taskId,
      int destination,
      uint64_t maxBytes,
      int64_t sequence,
      UcxDataAvailableCallbackV2 notify);

  /// Returns true if the given task can use intra-node transfer.
  /// Returns false until the task queue is initialized. Initialized broadcast
  /// queues are safe because shared pages are cloned by UcxExchangeSource.
  bool canUseIntraNode(std::string_view taskId);

  std::string describeQueueForIntraNode(std::string_view taskId);

  /// @brief Removes the queue for the given task from the queue manager.
  /// Calls "terminate" on the queue to awake waiting producers.
  void removeTask(std::string_view taskId);

  /// Process-registry observability for active queues and bounded lifecycle
  /// state. Historical task IDs are never retained.
  RegistryStats registryStats() const;

  /// @brief Returns the queue statistics of the queue associated with the given
  /// task. Returns nullopt when the specified output queue doesn't exist.
  std::optional<exec::OutputBuffer::Stats> stats(std::string_view taskId);

 private:
  struct HandshakeReservationKey {
    std::string taskId;
    uint32_t destination;
    uintptr_t endpointIdentity;

    bool operator==(const HandshakeReservationKey& other) const {
      return taskId == other.taskId && destination == other.destination &&
          endpointIdentity == other.endpointIdentity;
    }
  };

  struct HandshakeReservationKeyHash {
    size_t operator()(const HandshakeReservationKey& key) const;
  };

  void releaseHandshake(
      std::string_view taskId,
      uint32_t destination,
      uintptr_t endpointIdentity,
      uint64_t reservationId) noexcept;
  void releaseTaskHandshakes(std::string_view taskId) noexcept;
  void releaseTaskHandshakesLocked(std::string_view taskId) noexcept;
  UcxTaskLifecycleRegistry::TaskContract reconcileOutputBufferContract(
      std::string_view taskId,
      uint32_t numBuffers);

  // Retrieves the queue for a task if it exists.
  // Returns NULL if task not found.
  std::shared_ptr<UcxOutputQueue> getQueueIfExists(std::string_view taskId);

  // Throws an exception if queue doesn't exist.
  std::shared_ptr<UcxOutputQueue> getQueue(std::string_view taskId);

  folly::Synchronized<
      std::unordered_map<std::string, std::shared_ptr<UcxOutputQueue>>,
      std::mutex>
      queues_;

  UcxTaskLifecycleRegistry taskLifecycle_;

  // Serializes declaration/retirement so a delayed retirement for one task ID
  // cannot cancel the token allocated to its replacement incarnation.
  mutable std::mutex taskIncarnationMutex_;

  const size_t activeHandshakeCapacity_;
  const size_t activeHandshakesPerTaskCapacity_;
  mutable std::mutex handshakeMutex_;
  std::unordered_map<
      HandshakeReservationKey,
      uint64_t,
      HandshakeReservationKeyHash>
      activeHandshakes_;
  std::unordered_map<std::string, size_t> activeHandshakesPerTask_;
  std::unordered_map<std::string, TaskToken> intraNodeTaskTokens_;
  uint64_t totalHandshakeReservations_{0};
  uint64_t totalDuplicateHandshakes_{0};
  uint64_t totalHandshakeReservationRejected_{0};
  uint64_t nextHandshakeReservationId_{1};

  std::atomic<uint64_t> totalInitializeCalls_{0};
  std::atomic<uint64_t> totalRemoveCalls_{0};
  std::atomic<uint64_t> totalQueuesRemoved_{0};
};

} // namespace facebook::velox::ucx_exchange
