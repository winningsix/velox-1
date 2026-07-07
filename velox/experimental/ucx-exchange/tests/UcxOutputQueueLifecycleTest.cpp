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

#include <atomic>
#include <barrier>
#include <chrono>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::ucx_exchange {
namespace {

using namespace std::chrono_literals;

UcxTaskLifecycleRegistry::Options lifecycleOptions(
    size_t expectedCapacity,
    size_t pendingCapacity,
    uint32_t destinationLimit,
    std::chrono::milliseconds wait,
    size_t activeHandshakeCapacity = 65536,
    size_t activeHandshakesPerTaskCapacity = 65536) {
  UcxTaskLifecycleRegistry::Options options;
  options.expectedTaskCapacity = expectedCapacity;
  options.pendingRequestCapacity = pendingCapacity;
  options.maxDestinationsPerTask = destinationLimit;
  options.unknownTaskWait = wait;
  options.activeHandshakeCapacity = activeHandshakeCapacity;
  options.activeHandshakesPerTaskCapacity = activeHandshakesPerTaskCapacity;
  return options;
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

TEST(UcxOutputQueueLifecycleTest, EarlyRequestIsAdoptedAfterExpect) {
  auto manager = UcxOutputQueueManager::create(lifecycleOptions(8, 8, 64, 1s));
  std::atomic<bool> callbackFired{false};
  std::atomic<bool> receivedEnd{false};

  manager->getData(
      "early-task",
      0,
      [&](std::shared_ptr<cudf::packed_columns> data, std::vector<int64_t>) {
        receivedEnd.store(data == nullptr, std::memory_order_release);
        callbackFired.store(true, std::memory_order_release);
      });
  EXPECT_FALSE(callbackFired.load(std::memory_order_acquire));
  EXPECT_EQ(manager->registryStats().pendingUnknownRequests, 1);
  EXPECT_EQ(manager->registryStats().activeQueues, 0);

  manager->expectTask(
      "early-task", 1, core::PartitionedOutputNode::Kind::kPartitioned);
  EXPECT_EQ(manager->registryStats().pendingUnknownRequests, 0);
  EXPECT_EQ(manager->registryStats().activeQueues, 1);
  manager->removeTask("early-task");
  EXPECT_TRUE(callbackFired.load(std::memory_order_acquire));
  EXPECT_TRUE(receivedEnd.load(std::memory_order_acquire));
  EXPECT_EQ(manager->registryStats().activeQueues, 0);
  EXPECT_EQ(manager->registryStats().activeExpectedTasks, 0);
}

TEST(UcxOutputQueueLifecycleTest, LateRequestCannotRecreateRetiredQueue) {
  auto manager =
      UcxOutputQueueManager::create(lifecycleOptions(8, 8, 64, 10ms));
  manager->expectTask(
      "retired-task", 1, core::PartitionedOutputNode::Kind::kPartitioned);
  manager->removeTask("retired-task");
  std::atomic<bool> callbackFired{false};
  std::atomic<bool> receivedEnd{false};

  manager->getData(
      "retired-task",
      0,
      [&](std::shared_ptr<cudf::packed_columns> data, std::vector<int64_t>) {
        receivedEnd.store(data == nullptr, std::memory_order_release);
        callbackFired.store(true, std::memory_order_release);
      });
  EXPECT_TRUE(waitUntil(
      [&]() { return callbackFired.load(std::memory_order_acquire); }));
  EXPECT_TRUE(receivedEnd.load(std::memory_order_acquire));
  const auto stats = manager->registryStats();
  EXPECT_EQ(stats.activeQueues, 0);
  EXPECT_EQ(stats.activeExpectedTasks, 0);
  EXPECT_EQ(stats.pendingUnknownRequests, 0);
  EXPECT_EQ(stats.totalAdopted, 0);
  EXPECT_EQ(stats.totalExpired, 1);
}

TEST(UcxOutputQueueLifecycleTest, HandshakeAdmissionEnforcesDestinationLimit) {
  auto manager = UcxOutputQueueManager::create(lifecycleOptions(8, 8, 4, 1s));
  manager->expectTask(
      "bounded-task", 2, core::PartitionedOutputNode::Kind::kPartitioned);

  const auto accepted =
      manager->admitHandshake("bounded-task", 1, []() {}, [](auto) {});
  EXPECT_EQ(
      accepted.disposition,
      UcxTaskLifecycleRegistry::RequestDisposition::kExpected);
  EXPECT_EQ(accepted.contract.destinationCount, 2);

  const auto rejected = manager->admitHandshake(
      "bounded-task",
      std::numeric_limits<uint32_t>::max(),
      []() {},
      [](auto) {});
  EXPECT_EQ(
      rejected.disposition,
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected);
  EXPECT_EQ(
      rejected.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kInvalidDestination);
  EXPECT_EQ(manager->registryStats().activeQueues, 0);
}

TEST(UcxOutputQueueLifecycleTest, CapacityAndExpiryAreExplicitAndExactlyOnce) {
  auto manager = UcxOutputQueueManager::create(lifecycleOptions(8, 1, 4, 10ms));
  std::atomic<size_t> accepted{0};
  std::atomic<size_t> rejected{0};
  std::atomic<UcxTaskLifecycleRegistry::AdmissionRejectReason> reason{
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kNone};

  const auto deferred = manager->admitHandshake(
      "unknown-1",
      0,
      [&]() { accepted.fetch_add(1, std::memory_order_relaxed); },
      [&](auto value) {
        reason.store(value, std::memory_order_release);
        rejected.fetch_add(1, std::memory_order_release);
      });
  EXPECT_EQ(
      deferred.disposition,
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred);

  const auto capacity =
      manager->admitHandshake("unknown-2", 0, []() {}, [](auto) {});
  EXPECT_EQ(
      capacity.disposition,
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected);
  EXPECT_EQ(
      capacity.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kCapacity);

  EXPECT_TRUE(waitUntil(
      [&]() { return rejected.load(std::memory_order_acquire) == 1; }));
  EXPECT_EQ(accepted.load(std::memory_order_acquire), 0);
  EXPECT_EQ(rejected.load(std::memory_order_acquire), 1);
  EXPECT_EQ(
      reason.load(std::memory_order_acquire),
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kExpired);
  EXPECT_EQ(manager->registryStats().pendingUnknownRequests, 0);
  manager.reset();
  EXPECT_EQ(rejected.load(std::memory_order_acquire), 1);
}

TEST(UcxOutputQueueLifecycleTest, ReservationsAreBoundedAndDeduplicated) {
  auto manager =
      UcxOutputQueueManager::create(lifecycleOptions(8, 8, 8, 1s, 3, 2));
  manager->expectTask(
      "task-a", 4, core::PartitionedOutputNode::Kind::kPartitioned);
  manager->expectTask(
      "task-b", 4, core::PartitionedOutputNode::Kind::kPartitioned);

  auto a0 = manager->reserveHandshake("task-a", 0, 1);
  ASSERT_TRUE(a0);
  auto duplicate = manager->reserveHandshake("task-a", 0, 1);
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(
      duplicate.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kDuplicate);

  auto a1 = manager->reserveHandshake("task-a", 1, 1);
  ASSERT_TRUE(a1);
  auto perTaskCapacity = manager->reserveHandshake("task-a", 2, 1);
  EXPECT_FALSE(perTaskCapacity);
  EXPECT_EQ(
      perTaskCapacity.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kCapacity);

  auto b0 = manager->reserveHandshake("task-b", 0, 2);
  ASSERT_TRUE(b0);
  auto globalCapacity = manager->reserveHandshake("task-b", 1, 2);
  EXPECT_FALSE(globalCapacity);
  EXPECT_EQ(
      globalCapacity.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kCapacity);
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 3);

  a0.reservation.reset();
  auto b1 = manager->reserveHandshake("task-b", 1, 2);
  ASSERT_TRUE(b1);
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 3);

  manager->removeTask("task-a");
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 2);
  a1.reservation.reset();
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 2);
  b0.reservation.reset();
  b1.reservation.reset();
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 0);
}

TEST(UcxOutputQueueLifecycleTest, PausedResponderFloodCannotExceedCap) {
  auto manager =
      UcxOutputQueueManager::create(lifecycleOptions(8, 8, 128, 1s, 8, 4));
  manager->expectTask(
      "flood-task", 64, core::PartitionedOutputNode::Kind::kPartitioned);

  std::mutex reservationsMutex;
  std::vector<std::shared_ptr<UcxOutputQueueManager::HandshakeReservation>>
      reservations;
  std::vector<std::thread> clients;
  constexpr size_t kClients = 64;
  clients.reserve(kClients);
  for (size_t i = 0; i < kClients; ++i) {
    clients.emplace_back([&, i]() {
      auto result = manager->reserveHandshake(
          "flood-task",
          static_cast<uint32_t>(i),
          static_cast<uintptr_t>(i + 1));
      if (result) {
        std::lock_guard<std::mutex> lock(reservationsMutex);
        reservations.push_back(std::move(result.reservation));
      }
    });
  }
  for (auto& client : clients) {
    client.join();
  }

  // Holding every accepted reservation models a paused Communicator: no
  // responder/server reaches terminal and therefore no slot can be recycled.
  // Acceptor cannot enqueue a responder without one of these reservations.
  EXPECT_EQ(reservations.size(), 4);
  const auto stats = manager->registryStats();
  EXPECT_EQ(stats.activeHandshakeReservations, 4);
  EXPECT_LE(stats.activeHandshakeReservations, stats.activeHandshakeCapacity);
  EXPECT_GE(stats.totalHandshakeReservationRejected, kClients - 4);

  reservations.clear();
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 0);
}

TEST(UcxOutputQueueLifecycleTest, RetiredReservationCannotReleaseReusedTuple) {
  auto manager =
      UcxOutputQueueManager::create(lifecycleOptions(8, 8, 8, 1s, 4, 4));
  manager->expectTask(
      "reused-task", 1, core::PartitionedOutputNode::Kind::kPartitioned);
  auto retired = manager->reserveHandshake("reused-task", 0, 7);
  ASSERT_TRUE(retired);
  const auto retiredToken = retired.reservation->taskToken();
  ASSERT_TRUE(retiredToken);

  manager->removeTask("reused-task");
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 0);

  manager->expectTask(
      "reused-task", 1, core::PartitionedOutputNode::Kind::kPartitioned);
  auto current = manager->reserveHandshake("reused-task", 0, 7);
  ASSERT_TRUE(current);
  const auto currentToken = current.reservation->taskToken();
  EXPECT_EQ(currentToken.taskId, retiredToken.taskId);
  EXPECT_NE(currentToken.epoch, retiredToken.epoch);
  EXPECT_EQ(manager->taskToken("reused-task"), currentToken);
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 1);

  retired.reservation.reset();
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 1);
  auto duplicate = manager->reserveHandshake("reused-task", 0, 7);
  EXPECT_FALSE(duplicate);
  EXPECT_EQ(
      duplicate.rejectReason,
      UcxTaskLifecycleRegistry::AdmissionRejectReason::kDuplicate);

  current.reservation.reset();
  EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 0);
}

TEST(UcxOutputQueueLifecycleTest, ReserveAndRetireLeaveNoOrphan) {
  auto manager =
      UcxOutputQueueManager::create(lifecycleOptions(8, 8, 8, 1s, 4, 4));
  constexpr size_t kIterations = 200;
  for (size_t i = 0; i < kIterations; ++i) {
    manager->expectTask(
        "retire-race", 1, core::PartitionedOutputNode::Kind::kPartitioned);
    UcxOutputQueueManager::HandshakeReservationResult reservation;
    std::barrier start{3};
    std::thread reserver([&]() {
      start.arrive_and_wait();
      reservation = manager->reserveHandshake("retire-race", 0, 1);
    });
    std::thread retirer([&]() {
      start.arrive_and_wait();
      manager->removeTask("retire-race");
    });
    start.arrive_and_wait();
    reserver.join();
    retirer.join();

    EXPECT_EQ(manager->registryStats().activeHandshakeReservations, 0)
        << "iteration " << i;
    reservation.reservation.reset();
  }
}

} // namespace
} // namespace facebook::velox::ucx_exchange
