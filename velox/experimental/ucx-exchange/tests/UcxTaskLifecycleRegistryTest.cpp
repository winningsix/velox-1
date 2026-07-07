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
#include <string>
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

TEST(UcxTaskLifecycleRegistryTest, HandshakeBeforeExpectIsAdopted) {
  UcxTaskLifecycleRegistry registry({8, 8, 1s});
  std::atomic<size_t> adopted{0};
  std::atomic<size_t> expired{0};

  EXPECT_EQ(
      registry.deferIfUnexpected(
          "producer-task",
          [&]() { adopted.fetch_add(1, std::memory_order_relaxed); },
          [&]() { expired.fetch_add(1, std::memory_order_relaxed); }),
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred);
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 1);

  EXPECT_TRUE(registry.expectTask("producer-task"));
  EXPECT_EQ(adopted.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(expired.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
  EXPECT_EQ(registry.stats().totalAdopted, 1);
}

TEST(UcxTaskLifecycleRegistryTest, RetiredTaskNeverSelfReactivates) {
  UcxTaskLifecycleRegistry registry({8, 8, 10ms});
  EXPECT_TRUE(registry.expectTask("retired-task"));
  EXPECT_TRUE(registry.retireTask("retired-task"));

  std::atomic<size_t> adopted{0};
  std::atomic<size_t> expired{0};
  EXPECT_EQ(
      registry.deferIfUnexpected(
          "retired-task",
          [&]() { adopted.fetch_add(1, std::memory_order_relaxed); },
          [&]() { expired.fetch_add(1, std::memory_order_relaxed); }),
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred);
  EXPECT_TRUE(waitUntil([&]() {
    return expired.load(std::memory_order_relaxed) == 1;
  }));
  EXPECT_EQ(adopted.load(std::memory_order_relaxed), 0);
  EXPECT_FALSE(registry.isExpected("retired-task"));
  EXPECT_EQ(registry.stats().pendingUnknownRequests, 0);
}

TEST(UcxTaskLifecycleRegistryTest, CapacitiesRejectWithoutGrowth) {
  UcxTaskLifecycleRegistry registry({2, 2, 1s});
  EXPECT_TRUE(registry.expectTask("task-1"));
  EXPECT_TRUE(registry.expectTask("task-2"));
  EXPECT_THROW(
      registry.expectTask("task-3"), UcxTaskLifecycleCapacityError);
  EXPECT_TRUE(registry.retireTask("task-1"));
  EXPECT_TRUE(registry.expectTask("task-3"));

  const auto defer = [&](const std::string& taskId) {
    return registry.deferIfUnexpected(taskId, []() {}, []() {});
  };
  EXPECT_EQ(
      defer("unknown-1"),
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred);
  EXPECT_EQ(
      defer("unknown-2"),
      UcxTaskLifecycleRegistry::RequestDisposition::kDeferred);
  EXPECT_EQ(
      defer("unknown-3"),
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected);
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeExpectedTasks, 2);
  EXPECT_EQ(stats.pendingUnknownRequests, 2);
  EXPECT_EQ(stats.totalRejected, 2);
}

TEST(UcxTaskLifecycleRegistryTest, SequentialChurnRetainsNoHistory) {
  UcxTaskLifecycleRegistry registry({4, 4, 1s});
  constexpr size_t kIterations = 100000;
  for (size_t i = 0; i < kIterations; ++i) {
    const auto taskId = "task-" + std::to_string(i);
    ASSERT_TRUE(registry.expectTask(taskId));
    ASSERT_TRUE(registry.retireTask(taskId));
  }
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeExpectedTasks, 0);
  EXPECT_EQ(stats.pendingUnknownRequests, 0);
  EXPECT_EQ(stats.totalExpected, kIterations);
  EXPECT_EQ(stats.totalRetired, kIterations);
}

TEST(UcxTaskLifecycleRegistryTest, ConcurrentExpectRetireRetainsNoHistory) {
  UcxTaskLifecycleRegistry registry({64, 8, 1s});
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
            registry.expectTask(taskId);
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
