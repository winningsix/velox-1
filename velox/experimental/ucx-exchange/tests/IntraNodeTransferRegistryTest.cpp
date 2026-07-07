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

#include "velox/experimental/ucx-exchange/IntraNodeTransferRegistry.h"
#include "velox/experimental/ucx-exchange/UcxExchangeProtocol.h"

#include <gtest/gtest.h>
#include <rmm/cuda_stream_view.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

using namespace facebook::velox::ucx_exchange;
using namespace std::chrono_literals;

namespace {

TaskToken registerTask(
    const std::shared_ptr<IntraNodeTransferRegistry>& registry,
    const std::string& taskId) {
  return registry->expectTask(taskId).token;
}

IntraNodeTransferKey makeKey(
    TaskToken token,
    uint32_t sequenceNumber = 0) {
  return IntraNodeTransferKey{
      std::move(token), /*destination=*/0, sequenceNumber};
}

bool isCancellationStatus(IntraNodeTransferStatus status) {
  return status == IntraNodeTransferStatus::kCancelled ||
      status == IntraNodeTransferStatus::kAlreadyConsumed;
}

} // namespace

TEST(IntraNodeTransferRegistryTest, registerWaiterWokenByPublish) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "registerWaiterWokenByPublish");
  const auto key = makeKey(token);

  std::atomic<int> woken{0};
  std::atomic<int> retrieved{0};
  EXPECT_FALSE(registry->registerWaiter(key, [&woken]() { ++woken; }));

  auto future = registry->publish(
      key,
      /*data=*/nullptr,
      rmm::cuda_stream_default,
      /*atEnd=*/false,
      [&retrieved]() { ++retrieved; });
  EXPECT_EQ(woken.load(), 1);
  EXPECT_EQ(future.wait_for(0ms), std::future_status::timeout);

  auto result = registry->poll(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->status, IntraNodeTransferStatus::kData);
  EXPECT_EQ(future.wait_for(100ms), std::future_status::ready);
  EXPECT_EQ(retrieved.load(), 1);
  registry->cancelTask(token);
}

TEST(IntraNodeTransferRegistryTest, registerWaiterReadyReturnsTrueForRealEnd) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "registerWaiterReadyReturnsTrue");
  const auto key = makeKey(token);

  auto future = registry->publish(
      key, /*data=*/nullptr, rmm::cuda_stream_default, /*atEnd=*/true);
  std::atomic<int> woken{0};
  EXPECT_TRUE(registry->registerWaiter(key, [&woken]() { ++woken; }));
  EXPECT_EQ(woken.load(), 0);

  auto result = registry->poll(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->status, IntraNodeTransferStatus::kEnd);
  EXPECT_EQ(future.wait_for(100ms), std::future_status::ready);
  registry->cancelTask(token);
}

TEST(IntraNodeTransferRegistryTest, registerWaiterWokenByCancelIsNotEos) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "registerWaiterWokenByCancel");
  const auto key = makeKey(token);

  std::atomic<int> woken{0};
  EXPECT_FALSE(registry->registerWaiter(key, [&woken]() { ++woken; }));
  registry->cancelTask(token);
  EXPECT_EQ(woken.load(), 1);

  auto result = registry->poll(key);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->status, IntraNodeTransferStatus::kCancelled);
  EXPECT_NE(result->status, IntraNodeTransferStatus::kEnd);
}

TEST(IntraNodeTransferRegistryTest, cancelWakesBlockingWaitWithin100ms) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "cancelWakesBlockingWaitWithin100ms");
  const auto key = makeKey(token);
  std::promise<void> entered;

  auto waiter = std::async(std::launch::async, [&]() {
    entered.set_value();
    return registry->waitFor(key, 5s);
  });
  entered.get_future().wait();
  std::this_thread::sleep_for(10ms);

  const auto cancelStart = std::chrono::steady_clock::now();
  registry->cancelTask(token);
  ASSERT_EQ(waiter.wait_for(100ms), std::future_status::ready);
  const auto elapsed = std::chrono::steady_clock::now() - cancelStart;
  EXPECT_LT(elapsed, 100ms);
  EXPECT_TRUE(isCancellationStatus(waiter.get().status));
}

TEST(IntraNodeTransferRegistryTest, cancelNotifiesAllBlockingWaiters) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "cancelNotifiesAllBlockingWaiters");
  const auto key = makeKey(token);
  constexpr int kWaiters = 8;
  std::atomic<int> entered{0};
  std::vector<std::future<IntraNodeTransferResult>> waiters;
  waiters.reserve(kWaiters);
  for (int i = 0; i < kWaiters; ++i) {
    waiters.push_back(std::async(std::launch::async, [&]() {
      entered.fetch_add(1, std::memory_order_release);
      return registry->waitFor(key, 5s);
    }));
  }
  while (entered.load(std::memory_order_acquire) != kWaiters) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(10ms);
  registry->cancelTask(token);
  for (auto& waiter : waiters) {
    ASSERT_EQ(waiter.wait_for(100ms), std::future_status::ready);
    EXPECT_TRUE(isCancellationStatus(waiter.get().status));
  }
}

TEST(IntraNodeTransferRegistryTest, epochReuseFencesOldCancelAndPublish) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const std::string taskId = "epochReuseFencesOldCancelAndPublish";
  const auto oldToken = registerTask(registry, taskId);
  HandshakeResponse oldResponse;
  oldResponse.status = HandshakeStatus::kAccepted;
  oldResponse.destinationCount = 1;
  oldResponse.taskEpoch = oldToken.epoch;
  ASSERT_TRUE(isValidAcceptedHandshakeResponse(oldResponse, 0));
  const auto oldKey =
      makeKey(TaskToken{taskId, oldResponse.taskEpoch});
  auto oldFuture = registry->publish(
      oldKey, nullptr, rmm::cuda_stream_default, /*atEnd=*/false);
  registry->cancelTask(oldToken);
  EXPECT_EQ(oldFuture.wait_for(100ms), std::future_status::ready);

  const auto newToken = registerTask(registry, taskId);
  ASSERT_NE(oldToken.epoch, newToken.epoch);
  const auto newKey = makeKey(newToken);
  auto newFuture = registry->publish(
      newKey, nullptr, rmm::cuda_stream_default, /*atEnd=*/false);

  // Both a delayed cancel and a delayed publish from the old epoch are fenced.
  registry->cancelTask(oldToken);
  auto staleFuture = registry->publish(
      oldKey, nullptr, rmm::cuda_stream_default, /*atEnd=*/false);
  EXPECT_EQ(staleFuture.wait_for(0ms), std::future_status::ready);

  auto current = registry->poll(newKey);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(current->status, IntraNodeTransferStatus::kData);
  EXPECT_EQ(newFuture.wait_for(100ms), std::future_status::ready);

  auto stale = registry->poll(oldKey);
  ASSERT_TRUE(stale.has_value());
  EXPECT_EQ(stale->status, IntraNodeTransferStatus::kCancelled);
  registry->cancelTask(newToken);
}

TEST(IntraNodeTransferRegistryTest, concurrentConsumersFulfillOnlyOnce) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "concurrentConsumersFulfillOnlyOnce");
  const auto key = makeKey(token);
  std::atomic<int> retrieved{0};
  auto published = registry->publish(
      key,
      nullptr,
      rmm::cuda_stream_default,
      /*atEnd=*/false,
      [&retrieved]() { ++retrieved; });

  std::promise<void> start;
  auto gate = start.get_future().share();
  auto first = std::async(std::launch::async, [&]() {
    gate.wait();
    return registry->poll(key);
  });
  auto second = std::async(std::launch::async, [&]() {
    gate.wait();
    return registry->poll(key);
  });
  start.set_value();
  const auto firstResult = first.get();
  const auto secondResult = second.get();

  int dataResults = 0;
  for (const auto* result : {&firstResult, &secondResult}) {
    if (result->has_value() &&
        result->value().status == IntraNodeTransferStatus::kData) {
      ++dataResults;
    }
  }
  EXPECT_EQ(dataResults, 1);
  EXPECT_EQ(published.wait_for(100ms), std::future_status::ready);
  EXPECT_EQ(retrieved.load(), 1);
  registry->cancelTask(token);
}

TEST(IntraNodeTransferRegistryTest, duplicatePublishSharesEntryAndFuture) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token =
      registerTask(registry, "duplicatePublishSharesEntryAndFuture");
  const auto key = makeKey(token);
  std::atomic<int> retrieved{0};
  auto first = registry->publish(
      key,
      nullptr,
      rmm::cuda_stream_default,
      /*atEnd=*/false,
      [&retrieved]() { ++retrieved; });
  auto duplicate = registry->publish(
      key,
      nullptr,
      rmm::cuda_stream_default,
      /*atEnd=*/true,
      [&retrieved]() { ++retrieved; });

  const auto result = registry->poll(key);
  ASSERT_TRUE(result.has_value());
  // The duplicate cannot replace the first publication with a fake EOS.
  EXPECT_EQ(result->status, IntraNodeTransferStatus::kData);
  EXPECT_EQ(first.wait_for(100ms), std::future_status::ready);
  EXPECT_EQ(duplicate.wait_for(100ms), std::future_status::ready);
  EXPECT_EQ(retrieved.load(), 2);
  registry->cancelTask(token);
}

TEST(IntraNodeTransferRegistryTest, retrieveFulfillsSharedFutureOnce) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const auto token = registerTask(registry, "retrieveFulfillsSharedFutureOnce");
  const auto key = makeKey(token);
  std::atomic<int> retrieved{0};
  auto published = registry->publish(
      key,
      nullptr,
      rmm::cuda_stream_default,
      /*atEnd=*/false,
      [&retrieved]() { ++retrieved; });

  const auto first = registry->retrieve(key);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->status, IntraNodeTransferStatus::kData);
  EXPECT_FALSE(registry->retrieve(key).has_value());
  EXPECT_EQ(published.wait_for(100ms), std::future_status::ready);
  EXPECT_EQ(retrieved.load(), 1);
  registry->cancelTask(token);
}

TEST(IntraNodeTransferRegistryTest, publishCancelRaceStress) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const std::string taskId = "publishCancelRaceStress";
  constexpr uint32_t kIterations = 256;

  for (uint32_t iteration = 0; iteration < kIterations; ++iteration) {
    SCOPED_TRACE(iteration);
    const auto token = registerTask(registry, taskId);
    const auto key = makeKey(token, iteration);
    std::atomic<int> retrieved{0};
    std::promise<void> start;
    auto gate = start.get_future().share();

    auto publisher = std::async(std::launch::async, [&]() {
      gate.wait();
      return registry->publish(
          key,
          nullptr,
          rmm::cuda_stream_default,
          /*atEnd=*/false,
          [&retrieved]() { ++retrieved; });
    });
    auto canceller = std::async(std::launch::async, [&]() {
      gate.wait();
      registry->cancelTask(token);
    });
    start.set_value();

    auto published = publisher.get();
    canceller.get();
    EXPECT_EQ(published.wait_for(100ms), std::future_status::ready);
    EXPECT_EQ(retrieved.load(), 1);
    auto cancelled = registry->poll(key);
    ASSERT_TRUE(cancelled.has_value());
    EXPECT_EQ(cancelled->status, IntraNodeTransferStatus::kCancelled);
  }
}

TEST(IntraNodeTransferRegistryTest, cancelPollRaceNeverBecomesEos) {
  auto registry = IntraNodeTransferRegistry::getInstance();
  const std::string taskId = "cancelPollRaceNeverBecomesEos";
  constexpr uint32_t kIterations = 256;

  for (uint32_t iteration = 0; iteration < kIterations; ++iteration) {
    SCOPED_TRACE(iteration);
    const auto token = registerTask(registry, taskId);
    const auto key = makeKey(token, iteration);
    std::atomic<int> retrieved{0};
    auto published = registry->publish(
        key,
        nullptr,
        rmm::cuda_stream_default,
        /*atEnd=*/false,
        [&retrieved]() { ++retrieved; });
    std::promise<void> start;
    auto gate = start.get_future().share();
    auto consumer = std::async(std::launch::async, [&]() {
      gate.wait();
      return registry->poll(key);
    });
    auto canceller = std::async(std::launch::async, [&]() {
      gate.wait();
      registry->cancelTask(token);
    });
    start.set_value();

    const auto result = consumer.get();
    canceller.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(
        result->status == IntraNodeTransferStatus::kData ||
        result->status == IntraNodeTransferStatus::kCancelled);
    EXPECT_NE(result->status, IntraNodeTransferStatus::kEnd);
    EXPECT_EQ(published.wait_for(100ms), std::future_status::ready);
    EXPECT_EQ(retrieved.load(), 1);
  }
}
