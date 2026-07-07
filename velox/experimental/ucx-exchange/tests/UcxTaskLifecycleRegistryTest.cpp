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

#include <atomic>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::ucx_exchange {
namespace {

using namespace std::chrono_literals;
using Registry = UcxTaskLifecycleRegistry;

Registry::Options options(
    size_t expectedCapacity = 8,
    size_t pendingCapacity = 8,
    uint32_t destinationLimit = 64,
    std::chrono::milliseconds wait = 1s) {
  Registry::Options result;
  result.expectedTaskCapacity = expectedCapacity;
  result.pendingRequestCapacity = pendingCapacity;
  result.maxDestinationsPerTask = destinationLimit;
  result.unknownTaskWait = wait;
  return result;
}

Registry::TaskContract partitioned(uint32_t destinations = 1) {
  return {destinations, Registry::OutputKind::kPartitioned};
}

Registry::TaskContract broadcast(uint32_t destinations = 1) {
  return {destinations, Registry::OutputKind::kBroadcast};
}

template <typename Predicate>
bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = 1s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

TEST(UcxTaskLifecycleRegistryTest, HandshakeBeforeExpectIsAdopted) {
  Registry registry(options());
  std::atomic<size_t> adopted{0};
  std::atomic<size_t> rejected{0};

  const auto admission = registry.admitRequest(
      "producer-task",
      1,
      [&]() { adopted.fetch_add(1, std::memory_order_relaxed); },
      [&](Registry::AdmissionRejectReason) {
        rejected.fetch_add(1, std::memory_order_relaxed);
      });
  EXPECT_EQ(admission.disposition, Registry::RequestDisposition::kDeferred);
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 1);

  EXPECT_TRUE(registry.expectTask("producer-task", partitioned(2)));
  EXPECT_EQ(adopted.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(rejected.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
  EXPECT_EQ(registry.stats().totalAdopted, 1);
}

TEST(UcxTaskLifecycleRegistryTest, DeferredInvalidDestinationIsRejected) {
  Registry registry(options());
  std::atomic<size_t> accepted{0};
  std::atomic<size_t> rejected{0};
  std::atomic<Registry::AdmissionRejectReason> reason{
      Registry::AdmissionRejectReason::kNone};

  const auto admission = registry.admitRequest(
      "producer-task",
      2,
      [&]() { accepted.fetch_add(1, std::memory_order_relaxed); },
      [&](Registry::AdmissionRejectReason value) {
        reason.store(value, std::memory_order_release);
        rejected.fetch_add(1, std::memory_order_release);
      });
  ASSERT_EQ(admission.disposition, Registry::RequestDisposition::kDeferred);

  EXPECT_TRUE(registry.expectTask("producer-task", partitioned(2)));
  EXPECT_EQ(accepted.load(std::memory_order_acquire), 0);
  EXPECT_EQ(rejected.load(std::memory_order_acquire), 1);
  EXPECT_EQ(
      reason.load(std::memory_order_acquire),
      Registry::AdmissionRejectReason::kInvalidDestination);
  EXPECT_EQ(registry.stats().totalAdopted, 0);
  EXPECT_EQ(registry.stats().totalRejected, 1);
}

TEST(UcxTaskLifecycleRegistryTest, DestinationBoundaryIsHard) {
  Registry registry(options(8, 8, 4));
  ASSERT_TRUE(registry.expectTask("producer-task", partitioned(4)));

  const auto lastValid =
      registry.admitRequest("producer-task", 3, []() {}, [](auto) {});
  EXPECT_EQ(lastValid.disposition, Registry::RequestDisposition::kExpected);
  EXPECT_EQ(lastValid.contract.destinationCount, 4);

  const auto firstInvalid =
      registry.admitRequest("producer-task", 4, []() {}, [](auto) {});
  EXPECT_EQ(firstInvalid.disposition, Registry::RequestDisposition::kRejected);
  EXPECT_EQ(
      firstInvalid.rejectReason,
      Registry::AdmissionRejectReason::kInvalidDestination);

  const auto maxInvalid = registry.admitRequest(
      "producer-task",
      std::numeric_limits<uint32_t>::max(),
      []() {},
      [](auto) {});
  EXPECT_EQ(maxInvalid.disposition, Registry::RequestDisposition::kRejected);
  EXPECT_EQ(
      maxInvalid.rejectReason,
      Registry::AdmissionRejectReason::kInvalidDestination);
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
}

TEST(UcxTaskLifecycleRegistryTest, ExactContractAndBroadcastExpansion) {
  Registry registry(options(8, 8, 4));
  EXPECT_THROW(
      registry.expectTask("too-wide", partitioned(5)), std::invalid_argument);

  EXPECT_TRUE(registry.expectTask("partitioned", partitioned(2)));
  EXPECT_FALSE(registry.expectTask("partitioned", partitioned(2)));
  EXPECT_THROW(
      registry.expectTask("partitioned", partitioned(3)),
      std::invalid_argument);
  EXPECT_THROW(
      registry.expectTask("partitioned", broadcast(2)), std::invalid_argument);
  EXPECT_THROW(
      registry.expandBroadcastDestinations("partitioned", 3),
      std::invalid_argument);

  EXPECT_TRUE(registry.expectTask("broadcast", broadcast(1)));
  EXPECT_TRUE(registry.expandBroadcastDestinations("broadcast", 4));
  EXPECT_FALSE(registry.expandBroadcastDestinations("broadcast", 4));
  EXPECT_THROW(
      registry.expandBroadcastDestinations("broadcast", 3),
      std::invalid_argument);
  EXPECT_THROW(
      registry.expandBroadcastDestinations("broadcast", 5),
      std::invalid_argument);
}

TEST(UcxTaskLifecycleRegistryTest, RetiredTaskNeverSelfReactivates) {
  Registry registry(options(8, 8, 64, 10ms));
  EXPECT_TRUE(registry.expectTask("retired-task", partitioned()));
  EXPECT_TRUE(registry.retireTask("retired-task"));

  std::atomic<size_t> adopted{0};
  std::atomic<size_t> rejected{0};
  std::atomic<Registry::AdmissionRejectReason> reason{
      Registry::AdmissionRejectReason::kNone};
  const auto admission = registry.admitRequest(
      "retired-task",
      0,
      [&]() { adopted.fetch_add(1, std::memory_order_relaxed); },
      [&](Registry::AdmissionRejectReason value) {
        reason.store(value, std::memory_order_release);
        rejected.fetch_add(1, std::memory_order_release);
      });
  EXPECT_EQ(admission.disposition, Registry::RequestDisposition::kDeferred);
  EXPECT_TRUE(waitUntil(
      [&]() { return rejected.load(std::memory_order_acquire) == 1; }));
  EXPECT_EQ(adopted.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(
      reason.load(std::memory_order_acquire),
      Registry::AdmissionRejectReason::kExpired);
  EXPECT_FALSE(registry.isExpected("retired-task"));
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
}

TEST(UcxTaskLifecycleRegistryTest, CapacitiesRejectWithoutGrowth) {
  Registry registry(options(2, 2));
  EXPECT_TRUE(registry.expectTask("task-1", partitioned()));
  EXPECT_TRUE(registry.expectTask("task-2", partitioned()));
  EXPECT_THROW(
      registry.expectTask("task-3", partitioned()),
      UcxTaskLifecycleCapacityError);
  EXPECT_TRUE(registry.retireTask("task-1"));
  EXPECT_TRUE(registry.expectTask("task-3", partitioned()));

  const auto admit = [&](const std::string& taskId) {
    return registry.admitRequest(taskId, 0, []() {}, [](auto) {});
  };
  EXPECT_EQ(
      admit("unknown-1").disposition, Registry::RequestDisposition::kDeferred);
  EXPECT_EQ(
      admit("unknown-2").disposition, Registry::RequestDisposition::kDeferred);
  const auto rejected = admit("unknown-3");
  EXPECT_EQ(rejected.disposition, Registry::RequestDisposition::kRejected);
  EXPECT_EQ(rejected.rejectReason, Registry::AdmissionRejectReason::kCapacity);
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeExpectedTasks, 2);
  EXPECT_EQ(stats.pendingUnknownRequests, 2);
  EXPECT_EQ(stats.totalRejected, 2);
}

TEST(UcxTaskLifecycleRegistryTest, CallbacksRunOutsideLockOnResolverThread) {
  Registry registry(options());
  const auto callerThread = std::this_thread::get_id();
  std::thread::id callbackThread;
  std::atomic<bool> callbackRan{false};

  ASSERT_EQ(
      registry
          .admitRequest(
              "producer-task",
              0,
              [&]() {
                // stats() takes the registry mutex. This would deadlock if
                // callbacks were invoked while admission still held it.
                EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
                callbackThread = std::this_thread::get_id();
                callbackRan.store(true, std::memory_order_release);
              },
              [](auto) {})
          .disposition,
      Registry::RequestDisposition::kDeferred);

  std::thread resolver([&]() {
    EXPECT_TRUE(registry.expectTask("producer-task", partitioned()));
  });
  const auto resolverThread = resolver.get_id();
  resolver.join();

  EXPECT_TRUE(callbackRan.load(std::memory_order_acquire));
  EXPECT_EQ(callbackThread, resolverThread);
  EXPECT_NE(callbackThread, callerThread);
}

TEST(
    UcxTaskLifecycleRegistryTest,
    ResolutionIsExactlyOnceAcrossExpectRetireRace) {
  Registry registry(options(256, 256));
  constexpr size_t kIterations = 100;
  for (size_t i = 0; i < kIterations; ++i) {
    const auto taskId = "race-" + std::to_string(i);
    std::atomic<size_t> callbacks{0};
    ASSERT_EQ(
        registry
            .admitRequest(
                taskId,
                0,
                [&]() { callbacks.fetch_add(1, std::memory_order_relaxed); },
                [&](auto) {
                  callbacks.fetch_add(1, std::memory_order_relaxed);
                })
            .disposition,
        Registry::RequestDisposition::kDeferred);

    std::atomic<bool> start{false};
    std::thread expect([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      registry.expectTask(taskId, partitioned());
    });
    std::thread retire([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      registry.retireTask(taskId);
    });
    start.store(true, std::memory_order_release);
    expect.join();
    retire.join();
    registry.retireTask(taskId);
    EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 1) << taskId;
  }
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
}

TEST(UcxTaskLifecycleRegistryTest, SequentialChurnRetainsNoHistory) {
  Registry registry(options(4, 4));
  constexpr size_t kIterations = 100000;
  for (size_t i = 0; i < kIterations; ++i) {
    const auto taskId = "task-" + std::to_string(i);
    ASSERT_TRUE(registry.expectTask(taskId, partitioned()));
    ASSERT_TRUE(registry.retireTask(taskId));
  }
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeExpectedTasks, 0);
  EXPECT_EQ(stats.pendingUnknownRequests, 0);
  EXPECT_EQ(stats.totalExpected, kIterations);
  EXPECT_EQ(stats.totalRetired, kIterations);
}

TEST(UcxTaskLifecycleRegistryTest, ConcurrentExpectRetireRetainsNoHistory) {
  Registry registry(options(64, 8));
  constexpr size_t kThreads = 8;
  constexpr size_t kIterations = 2000;
  std::vector<std::thread> workers;
  for (size_t thread = 0; thread < kThreads; ++thread) {
    workers.emplace_back([&, thread]() {
      for (size_t i = 0; i < kIterations; ++i) {
        const auto taskId =
            "task-" + std::to_string(thread) + "-" + std::to_string(i);
        while (true) {
          try {
            registry.expectTask(taskId, partitioned());
            break;
          } catch (const UcxTaskLifecycleCapacityError&) {
            std::this_thread::yield();
          }
        }
        registry.retireTask(taskId);
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeExpectedTasks, 0);
  EXPECT_EQ(stats.totalExpected, kThreads * kIterations);
  EXPECT_EQ(stats.totalRetired, kThreads * kIterations);
}

} // namespace
} // namespace facebook::velox::ucx_exchange
