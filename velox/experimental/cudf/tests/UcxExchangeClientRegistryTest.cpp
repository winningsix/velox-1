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

#include "velox/experimental/cudf/exec/UcxExchangeClientRegistry.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::cudf_velox {
namespace {

std::shared_ptr<ucx_exchange::UcxExchangeClient> makeClient(
    const std::string& /*taskId*/) {
  // The registry only observes shared/weak ownership and never dereferences a
  // client. An aliasing pointer keeps this unit test independent of UCX and
  // CUDA initialization while exercising the exact production type.
  auto owner = std::make_shared<int>(1);
  auto* client = reinterpret_cast<ucx_exchange::UcxExchangeClient*>(
      owner.get());
  return std::shared_ptr<ucx_exchange::UcxExchangeClient>(
      std::move(owner), client);
}

TEST(UcxExchangeClientRegistryTest, DuplicateCleanupIsIdempotent) {
  UcxExchangeClientRegistry registry;
  auto client = makeClient("task-duplicate");
  registry.registerClient("task-duplicate", "node-1", client);
  registry.registerClient("task-duplicate", "node-2", client);

  EXPECT_EQ(registry.cleanupTask("task-duplicate"), 2);
  EXPECT_EQ(registry.cleanupTask("task-duplicate"), 0);
  const auto stats = registry.stats();
  EXPECT_EQ(stats.activeEntries, 0);
  EXPECT_EQ(stats.expiredEntries, 0);
  EXPECT_EQ(stats.totalCleaned, 2);
  EXPECT_EQ(stats.totalCleanupCalls, 2);
}

TEST(UcxExchangeClientRegistryTest, ConcurrentCleanupRemovesEachKeyOnce) {
  UcxExchangeClientRegistry registry;
  auto client = makeClient("task-concurrent");
  constexpr size_t kNodes = 64;
  for (size_t i = 0; i < kNodes; ++i) {
    registry.registerClient(
        "task-concurrent", "node-" + std::to_string(i), client);
  }

  std::atomic<size_t> removed{0};
  std::vector<std::thread> cleaners;
  for (size_t i = 0; i < 8; ++i) {
    cleaners.emplace_back([&]() {
      removed.fetch_add(
          registry.cleanupTask("task-concurrent"),
          std::memory_order_relaxed);
    });
  }
  for (auto& cleaner : cleaners) {
    cleaner.join();
  }

  EXPECT_EQ(removed.load(std::memory_order_relaxed), kNodes);
  EXPECT_EQ(registry.stats().activeEntries, 0);
  EXPECT_EQ(registry.stats().totalCleaned, kNodes);
}

TEST(UcxExchangeClientRegistryTest, TerminalCleanupLeavesNoStaleKey) {
  UcxExchangeClientRegistry registry;
  auto first = makeClient("task-reused");
  registry.registerClient("task-reused", "node", first);
  ASSERT_EQ(registry.find("task-reused", "node"), first);
  EXPECT_EQ(registry.cleanupTask("task-reused"), 1);
  EXPECT_EQ(registry.find("task-reused", "node"), nullptr);

  auto second = makeClient("task-reused");
  registry.registerClient("task-reused", "node", second);
  EXPECT_EQ(registry.find("task-reused", "node"), second);
  EXPECT_EQ(registry.stats().activeEntries, 1);
  EXPECT_EQ(registry.stats().expiredEntries, 0);
}

TEST(UcxExchangeClientRegistryTest, ExpiredEntriesAreObservableAndCleaned) {
  UcxExchangeClientRegistry registry;
  auto client = makeClient("task-expired");
  registry.registerClient("task-expired", "node", client);
  client.reset();

  EXPECT_EQ(registry.stats().expiredEntries, 1);
  EXPECT_EQ(registry.cleanupTask("task-expired"), 1);
  const auto stats = registry.stats();
  EXPECT_EQ(stats.expiredEntries, 0);
  EXPECT_EQ(stats.totalExpired, 1);
  EXPECT_EQ(stats.totalCleaned, 1);
}

} // namespace
} // namespace facebook::velox::cudf_velox
