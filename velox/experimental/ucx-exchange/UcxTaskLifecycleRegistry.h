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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace facebook::velox::ucx_exchange {

class UcxTaskLifecycleCapacityError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// Bounded lifecycle state for UCX task IDs.
///
/// Requests may arrive before the producer peer enters PREPARE. Such unknown
/// requests are retained only up to a hard count and deadline, then failed
/// closed. Once a task is retired, a late request follows the same bounded
/// wait but can never recreate a queue unless a new explicit expectTask()
/// declaration adopts it.
class UcxTaskLifecycleRegistry {
 public:
  using Callback = std::function<void()>;

  enum class OutputKind : uint8_t {
    kPartitioned,
    kBroadcast,
    kArbitrary,
  };

  struct TaskContract {
    uint32_t destinationCount{0};
    OutputKind outputKind{OutputKind::kPartitioned};

    bool operator==(const TaskContract& other) const {
      return destinationCount == other.destinationCount &&
          outputKind == other.outputKind;
    }
  };

  enum class AdmissionRejectReason : uint8_t {
    kNone,
    kInvalidDestination,
    kDuplicate,
    kCapacity,
    kExpired,
    kRetired,
    kShutdown,
  };

  using RejectCallback = std::function<void(AdmissionRejectReason)>;

  struct Options {
    size_t expectedTaskCapacity{65536};
    size_t pendingRequestCapacity{4096};
    std::chrono::milliseconds unknownTaskWait{30000};
    uint32_t maxDestinationsPerTask{65536};
    size_t activeHandshakeCapacity{65536};
    size_t activeHandshakesPerTaskCapacity{65536};
  };

  struct Stats {
    size_t activeExpectedTasks{0};
    size_t pendingUnknownRequests{0};
    size_t expectedTaskCapacity{0};
    size_t pendingRequestCapacity{0};
    uint32_t maxDestinationsPerTask{0};
    uint64_t totalExpected{0};
    uint64_t totalRetired{0};
    uint64_t totalDeferred{0};
    uint64_t totalAdopted{0};
    uint64_t totalExpired{0};
    uint64_t totalRejected{0};
  };

  enum class RequestDisposition {
    kExpected,
    kDeferred,
    kRejected,
  };

  struct AdmissionResult {
    RequestDisposition disposition{RequestDisposition::kRejected};
    AdmissionRejectReason rejectReason{AdmissionRejectReason::kNone};
    TaskContract contract;
  };

  UcxTaskLifecycleRegistry();
  explicit UcxTaskLifecycleRegistry(Options options);
  UcxTaskLifecycleRegistry(const UcxTaskLifecycleRegistry&) = delete;
  UcxTaskLifecycleRegistry& operator=(const UcxTaskLifecycleRegistry&) = delete;
  ~UcxTaskLifecycleRegistry();

  /// Declares the exact output contract for a task and synchronously adopts any
  /// bounded early requests. Re-declaration is idempotent only when the full
  /// contract is identical.
  /// Returns true for a new declaration and false for an existing one.
  bool expectTask(std::string_view taskId, TaskContract contract);

  /// Atomically validates a destination against an expected task contract or
  /// stores an early request. The caller invokes onRejected for an immediate
  /// kRejected result. Deferred callbacks are resolved exactly once by
  /// expectTask(), retireTask(), the deadline reaper, or shutdown, never under
  /// the registry lock.
  AdmissionResult admitRequest(
      std::string_view taskId,
      uint32_t destination,
      Callback onExpected,
      RejectCallback onRejected);

  /// Expands a live broadcast task's exact destination contract before new
  /// destinations are exposed. Partitioned and arbitrary contracts are fixed.
  bool expandBroadcastDestinations(
      std::string_view taskId,
      uint32_t destinationCount);

  std::optional<TaskContract> taskContract(std::string_view taskId) const;

  /// Retires a task and fails any still-pending early requests closed.
  bool retireTask(std::string_view taskId);

  bool isExpected(std::string_view taskId) const;
  size_t reapExpired();
  Stats stats() const;

  /// Stops the lazy reaper and fails all pending requests closed. Idempotent.
  void shutdown() noexcept;

 private:
  using Clock = std::chrono::steady_clock;

  struct PendingRequest {
    Clock::time_point deadline;
    uint32_t destination;
    Callback onExpected;
    RejectCallback onRejected;
  };

  struct ResolvedRequest {
    Callback onExpected;
    RejectCallback onRejected;
    AdmissionRejectReason reason{AdmissionRejectReason::kNone};
    bool accepted{false};
  };

  static void invokeAll(std::vector<ResolvedRequest>& callbacks) noexcept;
  void reaperLoop();

  const Options options_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_map<std::string, TaskContract> expectedTasks_;
  std::unordered_map<std::string, std::vector<PendingRequest>> pending_;
  size_t pendingRequestCount_{0};
  uint64_t totalExpected_{0};
  uint64_t totalRetired_{0};
  uint64_t totalDeferred_{0};
  uint64_t totalAdopted_{0};
  uint64_t totalExpired_{0};
  uint64_t totalRejected_{0};
  bool stopping_{false};
  std::thread reaperThread_;
};

} // namespace facebook::velox::ucx_exchange
