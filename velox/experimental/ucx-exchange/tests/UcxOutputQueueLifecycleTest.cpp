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
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::ucx_exchange {
namespace {

using namespace std::chrono_literals;

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
  auto manager = UcxOutputQueueManager::create(
      UcxTaskLifecycleRegistry::Options{8, 8, 1s});
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

  manager->expectTask("early-task");
  EXPECT_EQ(manager->registryStats().pendingUnknownRequests, 0);
  EXPECT_EQ(manager->registryStats().activeQueues, 1);
  manager->removeTask("early-task");
  EXPECT_TRUE(callbackFired.load(std::memory_order_acquire));
  EXPECT_TRUE(receivedEnd.load(std::memory_order_acquire));
  EXPECT_EQ(manager->registryStats().activeQueues, 0);
  EXPECT_EQ(manager->registryStats().activeExpectedTasks, 0);
}

TEST(UcxOutputQueueLifecycleTest, LateRequestCannotRecreateRetiredQueue) {
  auto manager = UcxOutputQueueManager::create(
      UcxTaskLifecycleRegistry::Options{8, 8, 10ms});
  manager->expectTask("retired-task");
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
  EXPECT_TRUE(waitUntil([&]() {
    return callbackFired.load(std::memory_order_acquire);
  }));
  EXPECT_TRUE(receivedEnd.load(std::memory_order_acquire));
  const auto stats = manager->registryStats();
  EXPECT_EQ(stats.activeQueues, 0);
  EXPECT_EQ(stats.activeExpectedTasks, 0);
  EXPECT_EQ(stats.pendingUnknownRequests, 0);
  EXPECT_EQ(stats.totalAdopted, 0);
  EXPECT_EQ(stats.totalExpired, 1);
}

} // namespace
} // namespace facebook::velox::ucx_exchange
