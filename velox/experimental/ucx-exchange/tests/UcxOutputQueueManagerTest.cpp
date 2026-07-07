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
#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <folly/Executor.h>
#include <folly/synchronization/EventCount.h>
#include <gtest/gtest.h>
#include <rmm/device_buffer.hpp>
#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <thread>
#include <vector>
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/ucx-exchange/UcxExchange.h"
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"
#include "velox/experimental/ucx-exchange/UcxExchangeQueue.h"
#include "velox/experimental/ucx-exchange/tests/UcxTestHelpers.h"

using namespace facebook::velox::ucx_exchange;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::ucx_exchange {

class UcxExchangeClientTestPeer {
 public:
  static void addClosedSourceMetrics(
      UcxExchangeClient& client,
      const UcxExchangeSource::BackpressureMetrics& metrics) {
    std::lock_guard<std::mutex> lock(client.queue_->mutex());
    client.mergeClosedSourceMetricsLocked(metrics);
  }

  static void beginLateClosedSource(UcxExchangeClient& client) {
    std::lock_guard<std::mutex> lock(client.queue_->mutex());
    ++client.pendingClosedSourceMerges_;
  }

  static void finishLateClosedSource(
      UcxExchangeClient& client,
      const UcxExchangeSource::BackpressureMetrics& metrics) {
    client.finishLateClosedSource(&metrics);
  }

  static bool isClosing(UcxExchangeClient& client) {
    std::lock_guard<std::mutex> lock(client.queue_->mutex());
    return client.closeState_ == UcxExchangeClient::CloseState::kClosing;
  }

  static bool isClosed(UcxExchangeClient& client) {
    std::lock_guard<std::mutex> lock(client.queue_->mutex());
    return client.closeState_ == UcxExchangeClient::CloseState::kClosed;
  }
};

class UcxExchangeSourceTestPeer {
 public:
  static std::shared_ptr<UcxExchangeSource> createForBackpressureMetrics(
      const std::shared_ptr<UcxExchangeQueue>& queue) {
    return std::shared_ptr<UcxExchangeSource>(new UcxExchangeSource(
        /*communicator=*/{},
        "metrics-client",
        "localhost",
        0,
        PartitionKey{"metrics-source", 0},
        queue));
  }

  static bool enter(UcxExchangeSource& source, bool waitingForReceiveCredit) {
    return source.enterBackpressure(waitingForReceiveCredit);
  }

  static void markClosedAndSettle(UcxExchangeSource& source) {
    source.closed_.store(true, std::memory_order_release);
    source.leaveBackpressure(/*resumedByConsumer=*/false);
  }
};

class UcxExchangeTestPeer {
 public:
  static bool shouldRecordStats(bool atEnd, bool noMoreSplits) {
    return UcxExchange::shouldRecordExchangeClientStats(atEnd, noMoreSplits);
  }
};

} // namespace facebook::velox::ucx_exchange

class UcxOutputQueueManagerTest : public testing::Test {
 protected:
  UcxOutputQueueManagerTest() {}

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool();
    queueManager_ = UcxOutputQueueManager::getInstanceRef();
  }

  std::shared_ptr<Task> initializeTask(
      std::string_view taskId,
      int numDestinations,
      int numDrivers,
      bool cleanup = true,
      core::PartitionedOutputNode::Kind kind =
          core::PartitionedOutputNode::Kind::kPartitioned) {
    if (cleanup) {
      queueManager_->removeTask(taskId);
    }

    auto task = createSourceTask(taskId, pool_, UcxTestData::kTestRowType);

    queueManager_->initializeTask(task, kind, numDestinations, numDrivers);
    return task;
  }

  std::unique_ptr<cudf::packed_columns> makePackedColumns(std::size_t numRows) {
    rmm::cuda_stream_view stream = rmm::cuda_stream_default;
    // Create table directly without going through pack/unpack
    auto table = facebook::velox::ucx_exchange::makeTable(
        numRows, UcxTestData::kTestRowType, stream);
    auto cols = std::make_unique<cudf::packed_columns>(
        cudf::pack(table->view(), stream));
    stream.synchronize();
    return cols;
  }

  void enqueue(std::string_view taskId, vector_size_t size) {
    enqueue(taskId, 0, size);
  }

  // Returns the enqueued page byte size.
  void enqueue(std::string_view taskId, int destination, vector_size_t size) {
    auto data = makePackedColumns(size);
    queueManager_->enqueue(taskId, destination, std::move(data), size);
  }

  void noMoreData(std::string_view taskId) {
    queueManager_->noMoreData(taskId);
  }

  void fetch(
      std::string_view taskId,
      int destination,
      bool expectedEndMarker = false) {
    bool receivedData = false;
    queueManager_->getData(
        taskId,
        destination,
        [destination, expectedEndMarker, &receivedData](
            std::shared_ptr<cudf::packed_columns> data,
            std::vector<int64_t> remainingBytes) {
          ASSERT_EQ(expectedEndMarker, data == nullptr)
              << "for destination " << destination;
          receivedData = true;
        });
    ASSERT_TRUE(receivedData) << "for destination " << destination;
  }

  struct FetchResponse {
    std::shared_ptr<cudf::packed_columns> data;
    int64_t sequence{-1};
    std::vector<int64_t> remainingBytes;
  };

  FetchResponse fetchV1Data(const std::string& taskId, int destination) {
    bool received = false;
    FetchResponse response;
    queueManager_->getData(
        taskId,
        destination,
        [&](std::shared_ptr<cudf::packed_columns> data,
            std::vector<int64_t> remainingBytes) {
          received = true;
          response =
              FetchResponse{std::move(data), -1, std::move(remainingBytes)};
        });
    EXPECT_TRUE(received) << "for destination " << destination;
    return response;
  }

  FetchResponse fetchV2Data(
      const std::string& taskId,
      int destination,
      uint64_t maxBytes,
      int64_t sequence) {
    bool received = false;
    FetchResponse response;
    queueManager_->getData(
        taskId,
        destination,
        maxBytes,
        sequence,
        [&](std::shared_ptr<cudf::packed_columns> data,
            int64_t receivedSequence,
            std::vector<int64_t> remainingBytes) {
          received = true;
          response = FetchResponse{
              std::move(data), receivedSequence, std::move(remainingBytes)};
        });
    EXPECT_TRUE(received) << "for destination " << destination << " sequence "
                          << sequence;
    return response;
  }

  UcxDataAvailableCallback receiveEndMarker(
      int destination,
      bool& receivedEndMarker) {
    return [destination, &receivedEndMarker](
               std::shared_ptr<cudf::packed_columns> data,
               std::vector<int64_t> remainingBytes) {
      EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
      EXPECT_TRUE(data == nullptr) << "for destination " << destination;
      EXPECT_TRUE(remainingBytes.empty());
      receivedEndMarker = true;
    };
  }

  void fetchEndMarker(std::string_view taskId, int destination) {
    bool receivedData = false;
    queueManager_->getData(
        taskId, destination, receiveEndMarker(destination, receivedData));
    EXPECT_TRUE(receivedData) << "for destination " << destination;
    queueManager_->deleteResults(taskId, destination);
  }

  void deleteResults(std::string_view taskId, int destination) {
    queueManager_->deleteResults(taskId, destination);
  }

  void registerForEndMarker(
      std::string_view taskId,
      int destination,
      bool& receivedEndMarker) {
    receivedEndMarker = false;
    queueManager_->getData(
        taskId, destination, receiveEndMarker(destination, receivedEndMarker));
    EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
  }

  UcxDataAvailableCallback receiveData(int destination, bool& receivedData) {
    receivedData = false;
    return [destination, &receivedData](
               std::shared_ptr<cudf::packed_columns> data,
               std::vector<int64_t> /*remainingBytes*/) {
      EXPECT_FALSE(receivedData) << "for destination " << destination;
      EXPECT_TRUE(data != nullptr) << "for destination " << destination;
      receivedData = true;
    };
  }

  void registerForData(
      std::string_view taskId,
      int destination,
      bool& receivedData) {
    receivedData = false;
    queueManager_->getData(
        taskId, destination, receiveData(destination, receivedData));
    EXPECT_FALSE(receivedData) << "for destination " << destination;
  }

  void dataFetcher(
      std::string_view taskId,
      int destination,
      int64_t& fetchedPackedColumns,
      bool earlyTermination) {
    int64_t received{0};
    folly::Random::DefaultGenerator rng;
    rng.seed(destination);
    while (true) {
      if (earlyTermination && folly::Random().oneIn(200)) {
        queueManager_->deleteResults(taskId, destination);
        return;
      }
      bool atEnd{false};
      folly::EventCount dataWait;
      auto dataWaitKey = dataWait.prepareWait();
      queueManager_->getData(
          taskId,
          destination,
          [&](std::shared_ptr<cudf::packed_columns> data,
              std::vector<int64_t> /*remainingBytes*/) {
            if (data == nullptr) {
              atEnd = true;
            } else {
              received++;
            }
            dataWait.notify();
          });
      dataWait.wait(dataWaitKey);
      if (atEnd) {
        break;
      }
    }
    queueManager_->deleteResults(taskId, destination);
    // out of order requests are allowed (fetch after delete)
    {
      struct Response {
        std::shared_ptr<cudf::packed_columns> data;
        std::vector<int64_t> remainingBytes;
      };
      folly::Promise<Response> promise;
      auto future = promise.getSemiFuture();
      queueManager_->getData(
          taskId,
          destination,
          [&promise](
              std::shared_ptr<cudf::packed_columns> data,
              std::vector<int64_t> remainingBytes) {
            promise.setValue(
                Response{std::move(data), std::move(remainingBytes)});
          });
      future.wait();
      ASSERT_TRUE(future.isReady());
      auto& response = future.value();
      ASSERT_EQ(response.remainingBytes.size(), 0);
      ASSERT_EQ(response.data, nullptr);
    }

    fetchedPackedColumns = received;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<UcxOutputQueueManager> queueManager_;
};

TEST_F(UcxOutputQueueManagerTest, basicPartitioned) {
  vector_size_t size = 100;
  std::string taskId = "t0";
  auto task = initializeTask(taskId, 5 /* numDestinations*/, 1 /*numDrivers*/);

  ASSERT_FALSE(queueManager_->isFinished(taskId));

  // - enqueue one group per destination
  // - fetch and ask one group per destination
  // - enqueue one more group for destinations 0-2
  // - fetch and ask one group for destinations 0-2
  // - register end-marker callback for destination 3 and data callback for 4
  // - enqueue data for 4 and assert callback was called
  // - enqueue end marker for all destinations
  // - assert callback was called for destination 3
  // - fetch end markers for destinations 0-2 and 4

  for (int destination = 0; destination < 5; ++destination) {
    enqueue(taskId, destination, size);
  }
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; destination++) {
    fetch(taskId, destination);
  }

  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    enqueue(taskId, destination, size);
  }

  for (int destination = 0; destination < 3; destination++) {
    fetch(taskId, destination);
  }

  // destinations 0-2 received 2 packed_columns each
  // destinations 3-4 received 1 packed_columns each

  bool receivedEndMarker3;
  registerForEndMarker(taskId, 3, receivedEndMarker3);

  bool receivedData4;
  registerForData(taskId, 4, receivedData4);

  enqueue(taskId, 4, size);
  EXPECT_TRUE(receivedData4);

  noMoreData(taskId);
  EXPECT_TRUE(receivedEndMarker3);
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    fetchEndMarker(taskId, destination);
  }

  EXPECT_TRUE(task->isRunning());

  deleteResults(taskId, 3);
  fetchEndMarker(taskId, 4);
  EXPECT_TRUE(queueManager_->isFinished(taskId));

  queueManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(UcxOutputQueueManagerTest, v1RepeatedFetchAdvancesQueue) {
  const std::string taskId = "v1RepeatedFetch";
  const int destination = 0;

  initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);

  enqueue(taskId, destination, 10);
  enqueue(taskId, destination, 20);
  noMoreData(taskId);

  auto first = fetchV1Data(taskId, destination);
  ASSERT_NE(first.data, nullptr);
  EXPECT_EQ(first.remainingBytes.size(), 1);

  auto second = fetchV1Data(taskId, destination);
  ASSERT_NE(second.data, nullptr);
  EXPECT_TRUE(second.remainingBytes.empty());

  auto end = fetchV1Data(taskId, destination);
  EXPECT_EQ(end.data, nullptr);
  EXPECT_TRUE(end.remainingBytes.empty());

  queueManager_->deleteResults(taskId, destination);
  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

TEST_F(UcxOutputQueueManagerTest, v2FetchesSequencesZeroAndOne) {
  const std::string taskId = "v2SeqZeroOne";
  const int destination = 0;
  const auto maxBytes = std::numeric_limits<uint64_t>::max();

  initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);

  enqueue(taskId, destination, 10);
  enqueue(taskId, destination, 20);
  noMoreData(taskId);

  auto first = fetchV2Data(taskId, destination, maxBytes, 0);
  ASSERT_NE(first.data, nullptr);
  EXPECT_EQ(first.sequence, 0);
  EXPECT_EQ(first.remainingBytes.size(), 1);

  auto second = fetchV2Data(taskId, destination, maxBytes, 1);
  ASSERT_NE(second.data, nullptr);
  EXPECT_EQ(second.sequence, 1);
  EXPECT_TRUE(second.remainingBytes.empty());

  auto end = fetchV2Data(taskId, destination, maxBytes, 2);
  EXPECT_EQ(end.data, nullptr);
  EXPECT_EQ(end.sequence, 2);
  EXPECT_TRUE(end.remainingBytes.empty());

  queueManager_->deleteResults(taskId, destination);
  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

TEST_F(UcxOutputQueueManagerTest, v2OversizeRequestReturnsOneChunk) {
  const std::string taskId = "v2OversizeReturnsOneChunk";
  const int destination = 0;
  const vector_size_t rows = 100;

  initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);

  auto firstData = makePackedColumns(rows);
  const auto firstBytes = firstData->gpu_data->size();
  ASSERT_GT(firstBytes, 1);

  queueManager_->enqueue(taskId, destination, std::move(firstData), rows);
  enqueue(taskId, destination, rows);
  noMoreData(taskId);

  const uint64_t maxBytes = firstBytes - 1;
  auto first = fetchV2Data(taskId, destination, maxBytes, 0);
  ASSERT_NE(first.data, nullptr);
  EXPECT_EQ(first.sequence, 0);
  EXPECT_EQ(first.data->gpu_data->size(), firstBytes);
  EXPECT_EQ(first.remainingBytes.size(), 1);

  auto second =
      fetchV2Data(taskId, destination, std::numeric_limits<uint64_t>::max(), 1);
  ASSERT_NE(second.data, nullptr);
  EXPECT_EQ(second.sequence, 1);

  auto end =
      fetchV2Data(taskId, destination, std::numeric_limits<uint64_t>::max(), 2);
  EXPECT_EQ(end.data, nullptr);
  EXPECT_EQ(end.sequence, 2);

  queueManager_->deleteResults(taskId, destination);
  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

TEST_F(UcxOutputQueueManagerTest, v2TaskRemovalWakesAtRequestedSequence) {
  const std::string taskId = "v2RemovedTask";
  const int destination = 0;
  const int64_t requestedSequence = 7;

  auto task =
      initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);
  task->requestAbort().wait();
  queueManager_->removeTask(taskId);

  // A retired wire task cannot be distinguished from a new incarnation until
  // TaskToken epochs are added. New incarnations must therefore be declared
  // before admission; removeTask then wakes the already-admitted waiter while
  // preserving its requested sequence.
  queueManager_->expectTask(
      taskId, 1, core::PartitionedOutputNode::Kind::kPartitioned);
  bool received = false;
  FetchResponse response;
  queueManager_->getData(
      taskId,
      destination,
      std::numeric_limits<uint64_t>::max(),
      requestedSequence,
      [&](std::shared_ptr<cudf::packed_columns> data,
          int64_t receivedSequence,
          std::vector<int64_t> remainingBytes) {
        received = true;
        response = FetchResponse{
            std::move(data), receivedSequence, std::move(remainingBytes)};
      });
  EXPECT_FALSE(received);
  queueManager_->removeTask(taskId);

  EXPECT_TRUE(received);
  EXPECT_EQ(response.data, nullptr);
  EXPECT_EQ(response.sequence, requestedSequence);
  EXPECT_TRUE(response.remainingBytes.empty());
}

TEST_F(UcxOutputQueueManagerTest, exchangeQueueCloseWakesWaitingConsumer) {
  UcxExchangeQueue queue(1, UcxExchangeQueue::kMaxInflightReceiveBytesCeiling);
  bool atEnd = false;
  ContinueFuture future;
  ContinuePromise stalePromise = ContinuePromise::makeEmpty();

  {
    std::lock_guard<std::mutex> l(queue.mutex());
    auto data = queue.dequeueLocked(0, &atEnd, &future, &stalePromise);
    EXPECT_EQ(data, nullptr);
    EXPECT_FALSE(atEnd);
  }
  EXPECT_FALSE(stalePromise.valid());

  queue.close();
  future.wait();
  EXPECT_TRUE(future.isReady());
}

TEST_F(UcxOutputQueueManagerTest, receiveBudgetIsPerClientAndValidated) {
  constexpr int64_t kSmallCap = 48LL << 20;
  constexpr int64_t kLargeCap = 96LL << 20;
  EXPECT_EQ(
      QueryConfig({}).ucxMaxInflightReceiveBytesPerClient(),
      UcxExchangeQueue::kMaxInflightReceiveBytesCeiling);
  UcxExchangeClient smallClient("small-query", 0, 1, kSmallCap);
  UcxExchangeClient largeClient("large-query", 0, 1, kLargeCap);

  EXPECT_EQ(smallClient.queue()->maxInflightReceiveBytes(), kSmallCap);
  EXPECT_EQ(largeClient.queue()->maxInflightReceiveBytes(), kLargeCap);
  const auto initialStats = smallClient.stats();
  EXPECT_FALSE(initialStats.empty());
  EXPECT_EQ(
      initialStats.at(UcxExchangeClient::kMetricMaxInflightReceiveBytes).sum,
      kSmallCap);
  EXPECT_EQ(
      initialStats.at(UcxExchangeClient::kMetricCurrentInflightReceiveBytes)
          .sum,
      0);
  EXPECT_EQ(
      initialStats.at(UcxExchangeClient::kMetricPeakInflightReceiveBytes).sum,
      0);
  EXPECT_EQ(
      initialStats.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum, 0);

  EXPECT_TRUE(smallClient.queue()->tryReserveReceive(1024));
  const auto reservedStats = smallClient.stats();
  EXPECT_EQ(
      reservedStats.at(UcxExchangeClient::kMetricCurrentPendingReceiveBytes)
          .sum,
      1024);
  EXPECT_EQ(
      reservedStats.at(UcxExchangeClient::kMetricCurrentInflightReceiveBytes)
          .sum,
      1024);
  EXPECT_EQ(
      reservedStats.at(UcxExchangeClient::kMetricPeakInflightReceiveBytes).sum,
      1024);
  smallClient.queue()->releaseReservedReceive(1024);
  const auto releasedStats = smallClient.stats();
  EXPECT_EQ(
      releasedStats.at(UcxExchangeClient::kMetricCurrentInflightReceiveBytes)
          .sum,
      0);
  EXPECT_EQ(
      releasedStats.at(UcxExchangeClient::kMetricPeakInflightReceiveBytes).sum,
      1024);
  EXPECT_ANY_THROW(UcxExchangeQueue(1, 0));
  EXPECT_ANY_THROW(UcxExchangeQueue(1, -1));
  EXPECT_ANY_THROW(UcxExchangeQueue(
      1, UcxExchangeQueue::kMaxInflightReceiveBytesCeiling + 1));
}

TEST_F(UcxOutputQueueManagerTest, closedSourceStatsRemainMonotonic) {
  UcxExchangeClient client("closed-stats-query", 0, 1, 1024);
  UcxExchangeClientTestPeer::addClosedSourceMetrics(
      client,
      UcxExchangeSource::BackpressureMetrics{/*pauseCount=*/3,
                                             /*resumeCount=*/2,
                                             /*receiveCreditWaitCount=*/1,
                                             /*pausedNanos=*/100});

  const auto beforeClose = client.stats();
  EXPECT_EQ(
      beforeClose.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum, 3);
  EXPECT_EQ(
      beforeClose.at(UcxExchangeClient::kMetricBackpressureResumeCount).sum, 2);
  EXPECT_EQ(
      beforeClose.at(UcxExchangeClient::kMetricReceiveCreditWaitCount).sum, 1);
  EXPECT_EQ(
      beforeClose.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum,
      100);

  client.close();
  const auto afterClose = client.stats();
  EXPECT_FALSE(afterClose.empty());
  EXPECT_GE(
      afterClose.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum,
      beforeClose.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum);
  EXPECT_GE(
      afterClose.at(UcxExchangeClient::kMetricBackpressureResumeCount).sum,
      beforeClose.at(UcxExchangeClient::kMetricBackpressureResumeCount).sum);
  EXPECT_GE(
      afterClose.at(UcxExchangeClient::kMetricReceiveCreditWaitCount).sum,
      beforeClose.at(UcxExchangeClient::kMetricReceiveCreditWaitCount).sum);
  EXPECT_GE(
      afterClose.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum,
      beforeClose.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum);

  client.close();
  const auto afterRepeatedClose = client.stats();
  EXPECT_EQ(
      afterRepeatedClose.at(UcxExchangeClient::kMetricBackpressurePauseCount)
          .sum,
      afterClose.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum);
  EXPECT_EQ(
      afterRepeatedClose.at(UcxExchangeClient::kMetricBackpressurePausedNanos)
          .sum,
      afterClose.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum);

  // A late-retired source can merge after the initial close snapshot. Its
  // cumulative metrics must remain monotonic and must not wrap.
  UcxExchangeClientTestPeer::addClosedSourceMetrics(
      client,
      UcxExchangeSource::BackpressureMetrics{
          /*pauseCount=*/std::numeric_limits<uint64_t>::max(),
          /*resumeCount=*/std::numeric_limits<uint64_t>::max(),
          /*receiveCreditWaitCount=*/std::numeric_limits<uint64_t>::max(),
          /*pausedNanos=*/std::numeric_limits<uint64_t>::max()});
  UcxExchangeClientTestPeer::addClosedSourceMetrics(
      client, UcxExchangeSource::BackpressureMetrics{1, 1, 1, 1});
  const auto saturated = client.stats();
  EXPECT_EQ(
      saturated.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum,
      std::numeric_limits<int64_t>::max());
  EXPECT_EQ(
      saturated.at(UcxExchangeClient::kMetricBackpressureResumeCount).sum,
      std::numeric_limits<int64_t>::max());
  EXPECT_EQ(
      saturated.at(UcxExchangeClient::kMetricReceiveCreditWaitCount).sum,
      std::numeric_limits<int64_t>::max());
  EXPECT_EQ(
      saturated.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum,
      std::numeric_limits<int64_t>::max());
}

TEST_F(UcxOutputQueueManagerTest, statisticsCollectionIsTerminalOnly) {
  // Model a long stream of data-bearing calls. Data arrival alone must never
  // trigger the O(peers) source snapshot and 13-key runtimeStats allocation.
  for (int32_t chunk = 0; chunk < 10'000; ++chunk) {
    EXPECT_FALSE(
        UcxExchangeTestPeer::shouldRecordStats(
            /*atEnd=*/false, /*noMoreSplits=*/true));
  }
  EXPECT_FALSE(
      UcxExchangeTestPeer::shouldRecordStats(
          /*atEnd=*/true, /*noMoreSplits=*/false));
  EXPECT_TRUE(
      UcxExchangeTestPeer::shouldRecordStats(
          /*atEnd=*/true, /*noMoreSplits=*/true));
}

TEST_F(UcxOutputQueueManagerTest, concurrentCloseWaitsForFinalMetricsMerge) {
  using namespace std::chrono_literals;

  UcxExchangeClient client("close-barrier-query", 0, 1, 1024);
  std::atomic<bool> secondReturned{false};
  std::exception_ptr firstFailure;
  std::exception_ptr secondFailure;

  // Model an addRemoteTaskId() that observed Closing and is retiring its
  // source outside the queue lock. The first close must not publish Closed
  // until this source's metrics have merged.
  UcxExchangeClientTestPeer::beginLateClosedSource(client);

  std::thread first([&]() {
    try {
      client.close();
    } catch (...) {
      firstFailure = std::current_exception();
    }
  });
  const auto closingDeadline = std::chrono::steady_clock::now() + 2s;
  while (!UcxExchangeClientTestPeer::isClosing(client) &&
         std::chrono::steady_clock::now() < closingDeadline) {
    std::this_thread::sleep_for(1ms);
  }
  if (!UcxExchangeClientTestPeer::isClosing(client)) {
    UcxExchangeClientTestPeer::finishLateClosedSource(
        client, UcxExchangeSource::BackpressureMetrics{});
    first.join();
    FAIL() << "first close did not enter Closing";
  }

  std::thread second([&]() {
    try {
      client.close();
      secondReturned.store(true, std::memory_order_release);
    } catch (...) {
      secondFailure = std::current_exception();
    }
  });
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(secondReturned.load(std::memory_order_acquire));

  UcxExchangeClientTestPeer::finishLateClosedSource(
      client,
      UcxExchangeSource::BackpressureMetrics{/*pauseCount=*/3,
                                             /*resumeCount=*/2,
                                             /*receiveCreditWaitCount=*/1,
                                             /*pausedNanos=*/100});
  first.join();
  second.join();
  EXPECT_EQ(firstFailure, nullptr);
  EXPECT_EQ(secondFailure, nullptr);
  EXPECT_TRUE(secondReturned.load(std::memory_order_acquire));
  EXPECT_TRUE(UcxExchangeClientTestPeer::isClosed(client));
  const auto finalStats = client.stats();
  EXPECT_EQ(
      finalStats.at(UcxExchangeClient::kMetricBackpressurePauseCount).sum, 3);
  EXPECT_EQ(
      finalStats.at(UcxExchangeClient::kMetricBackpressureResumeCount).sum, 2);
  EXPECT_EQ(
      finalStats.at(UcxExchangeClient::kMetricReceiveCreditWaitCount).sum, 1);
  EXPECT_EQ(
      finalStats.at(UcxExchangeClient::kMetricBackpressurePausedNanos).sum,
      100);

  // A repeated close observes the completed barrier and remains idempotent.
  client.close();
  EXPECT_TRUE(UcxExchangeClientTestPeer::isClosed(client));
}

TEST_F(UcxOutputQueueManagerTest, backpressureCannotRestartAfterClose) {
  auto queue = std::make_shared<UcxExchangeQueue>(1, 1024);
  auto source = UcxExchangeSourceTestPeer::createForBackpressureMetrics(queue);

  EXPECT_TRUE(
      UcxExchangeSourceTestPeer::enter(
          *source, /*waitingForReceiveCredit=*/true));
  const auto active = source->backpressureMetrics();
  EXPECT_EQ(active.pauseCount, 1);
  EXPECT_EQ(active.receiveCreditWaitCount, 1);

  UcxExchangeSourceTestPeer::markClosedAndSettle(*source);
  const auto settled = source->backpressureMetrics();
  EXPECT_EQ(settled.pauseCount, 1);
  EXPECT_EQ(settled.resumeCount, 0);
  EXPECT_EQ(settled.receiveCreditWaitCount, 1);

  // A progress iteration that passed its first closed_ check before close()
  // cannot create a new dormant interval after the close linearization.
  EXPECT_FALSE(
      UcxExchangeSourceTestPeer::enter(
          *source, /*waitingForReceiveCredit=*/false));
  const auto afterStaleEnter = source->backpressureMetrics();
  EXPECT_EQ(afterStaleEnter.pauseCount, settled.pauseCount);
  EXPECT_EQ(afterStaleEnter.pausedNanos, settled.pausedNanos);
}

TEST_F(
    UcxOutputQueueManagerTest,
    postedReceiveCreditRemainsCurrentUntilAsyncSourceCleanup) {
  constexpr int64_t kPostedBytes = 128;
  UcxExchangeClient client("posted-close-query", 0, 1, 1024);
  ASSERT_TRUE(client.queue()->tryReserveReceive(kPostedBytes));

  client.close();
  const auto atClose = client.stats();
  EXPECT_EQ(atClose.at(UcxExchangeClient::kMetricCurrentQueuedBytes).sum, 0);
  EXPECT_EQ(
      atClose.at(UcxExchangeClient::kMetricCurrentPendingReceiveBytes).sum,
      kPostedBytes);
  EXPECT_EQ(
      atClose.at(UcxExchangeClient::kMetricCurrentInflightReceiveBytes).sum,
      kPostedBytes);
  EXPECT_EQ(
      atClose.at(UcxExchangeClient::kMetricPeakInflightReceiveBytes).sum,
      kPostedBytes);

  // Source cleanup runs on the communicator thread after close. Model its
  // reservation release and prove that current drops while peak is retained.
  client.queue()->releaseReservedReceive(kPostedBytes);
  const auto afterCleanup = client.stats();
  EXPECT_EQ(
      afterCleanup.at(UcxExchangeClient::kMetricCurrentPendingReceiveBytes).sum,
      0);
  EXPECT_EQ(
      afterCleanup.at(UcxExchangeClient::kMetricCurrentInflightReceiveBytes)
          .sum,
      0);
  EXPECT_EQ(
      afterCleanup.at(UcxExchangeClient::kMetricPeakInflightReceiveBytes).sum,
      kPostedBytes);
}

TEST_F(UcxOutputQueueManagerTest, receiveBudgetAggregatesAndResumesSafely) {
  constexpr int64_t kCap = 64;
  UcxExchangeQueue queue(1, kCap);
  UcxExchangeQueue::BackpressureStats stats;

  EXPECT_TRUE(queue.tryReserveReceive(40, &stats));
  EXPECT_EQ(stats.inFlightBytes, 40);
  EXPECT_FALSE(queue.tryReserveReceive(25, &stats));
  EXPECT_EQ(stats.inFlightBytes, 40);
  EXPECT_TRUE(queue.tryReserveReceive(24, &stats));
  EXPECT_EQ(stats.inFlightBytes, kCap);
  auto metrics = queue.metricsSnapshot();
  EXPECT_EQ(metrics.pendingReceiveBytes, kCap);
  EXPECT_EQ(metrics.inFlightBytes, kCap);
  EXPECT_EQ(metrics.peakInflightReceiveBytes, kCap);
  EXPECT_EQ(metrics.maxInflightReceiveBytes, kCap);

  queue.releaseReservedReceive(kCap);
  EXPECT_FALSE(queue.shouldPauseReceive(32, &stats));
  EXPECT_EQ(stats.inFlightBytes, 0);
  EXPECT_TRUE(queue.tryReserveReceive(kCap + 1, &stats));
  EXPECT_TRUE(queue.shouldPauseReceive(32, &stats));
  EXPECT_FALSE(queue.tryReserveReceive(1, &stats));
  metrics = queue.metricsSnapshot();
  EXPECT_EQ(metrics.inFlightBytes, kCap + 1);
  EXPECT_EQ(metrics.peakInflightReceiveBytes, kCap + 1);
  queue.releaseReservedReceive(kCap + 1);
  EXPECT_FALSE(queue.shouldPauseReceive(32, &stats));
  EXPECT_EQ(queue.metricsSnapshot().inFlightBytes, 0);
}

TEST_F(UcxOutputQueueManagerTest, oversizeFirstReceiveAvoidsSignedOverflow) {
  UcxExchangeQueue queue(1, 64);
  UcxExchangeQueue::BackpressureStats stats;
  constexpr auto kMax = std::numeric_limits<int64_t>::max();

  EXPECT_TRUE(queue.tryReserveReceive(kMax, &stats));
  EXPECT_EQ(stats.inFlightBytes, kMax);
  EXPECT_TRUE(queue.shouldPauseReceive(32, &stats));
  EXPECT_FALSE(queue.tryReserveReceive(kMax, &stats));
  EXPECT_EQ(stats.inFlightBytes, kMax);
  queue.releaseReservedReceive(kMax);
  EXPECT_FALSE(queue.shouldPauseReceive(32, &stats));
  EXPECT_EQ(stats.inFlightBytes, 0);
}

TEST_F(UcxOutputQueueManagerTest, basicAsyncFetch) {
  const vector_size_t size = 10;
  const std::string taskId = "t0";
  int numPartitions = 1;
  bool earlyTermination = false;

  initializeTask(taskId, numPartitions, 1);

  // asynchronously fetch data.
  int64_t fetchedPackedColumns = 0;
  std::thread thread([&]() {
    dataFetcher(taskId, 0, fetchedPackedColumns, earlyTermination);
  });
  const int totalPackedColumns = 100;
  int64_t producedPackedColumns = 0;

  for (int i = 0; i < totalPackedColumns; ++i) {
    const int partition = 0;
    try {
      enqueue(taskId, partition, size);
    } catch (...) {
      // Early termination might cause the task to fail early.
      ASSERT_TRUE(earlyTermination);
      break;
    }
    ++producedPackedColumns;
    if (folly::Random().oneIn(4)) {
      std::this_thread::sleep_for(std::chrono::microseconds(5)); // NOLINT
    }
  }
  noMoreData(taskId);

  thread.join();

  if (!earlyTermination) {
    ASSERT_EQ(fetchedPackedColumns, producedPackedColumns);
  }
  queueManager_->removeTask(taskId);
}

TEST_F(UcxOutputQueueManagerTest, lateTaskCreation) {
  const vector_size_t size = 10;
  const std::string taskId = "t0";
  int numPartitions = 1;
  bool earlyTermination = false;
  int destination = 0;

  // Declare the producer lifecycle before its queue is initialized.
  queueManager_->removeTask(taskId);
  queueManager_->expectTask(
      taskId,
      static_cast<uint32_t>(numPartitions),
      core::PartitionedOutputNode::Kind::kPartitioned);

  // Fetch data from a non-existing task.
  struct Response {
    std::shared_ptr<cudf::packed_columns> data;
    std::vector<int64_t> remainingBytes;
  };
  folly::Promise<Response> promise;
  auto future = promise.getSemiFuture();
  queueManager_->getData(
      taskId,
      destination,
      [&promise](
          std::shared_ptr<cudf::packed_columns> data,
          std::vector<int64_t> remainingBytes) {
        promise.setValue(Response{std::move(data), std::move(remainingBytes)});
      });
  // initialize task.
  auto task = initializeTask(taskId, numPartitions, 1, false);
  // enqueue some data.
  enqueue(taskId, destination, size);
  noMoreData(taskId);
  uint32_t i = 0;
  while (true) {
    ++i;
    future.wait();
    ASSERT_TRUE(future.isReady());
    Response& response = future.value();
    if (!response.data) {
      // nullptr -> end of transmission.
      break;
    }
    folly::Promise<Response> promise;
    future = promise.getSemiFuture();
    queueManager_->getData(
        taskId,
        destination,
        [&promise](
            std::shared_ptr<cudf::packed_columns> data,
            std::vector<int64_t> remainingBytes) {
          promise.setValue(
              Response{std::move(data), std::move(remainingBytes)});
        });
  }
  ASSERT_EQ(i, 2);

  queueManager_->deleteResults(taskId, destination);
  queueManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(UcxOutputQueueManagerTest, multiFetchers) {
  const std::vector<bool> earlyTerminations = {false, true};
  for (const auto earlyTermination : earlyTerminations) {
    SCOPED_TRACE(fmt::format("earlyTermination {}", earlyTermination));

    const vector_size_t size = 10;
    const std::string taskId = "t0";
    int numPartitions = 10;
    initializeTask(taskId, numPartitions, 1);

    // create one thread per partition to asynchronously fetch data.
    std::vector<std::thread> threads;
    std::vector<int64_t> fetchedPackedColumns(numPartitions, 0);

    for (size_t i = 0; i < numPartitions; ++i) {
      threads.emplace_back([&, i]() {
        dataFetcher(taskId, i, fetchedPackedColumns.at(i), earlyTermination);
      });
    }

    // enqueue packed columns.
    folly::Random::DefaultGenerator rng;
    rng.seed(1234);
    const int totalPackedColumns = 1000;
    std::vector<int64_t> producedPackedColumns(numPartitions, 0);
    for (int i = 0; i < totalPackedColumns; ++i) {
      // randomly chose a partition.
      const int partition = folly::Random().rand32(rng) % numPartitions;
      try {
        enqueue(taskId, partition, size);
      } catch (...) {
        // Early termination might cause the task to fail early.
        ASSERT_TRUE(earlyTermination);
        break;
      }
      ++producedPackedColumns[partition];
      if (folly::Random().oneIn(4)) {
        std::this_thread::sleep_for(std::chrono::microseconds(5)); // NOLINT
      }
    }
    noMoreData(taskId);

    for (int i = 0; i < threads.size(); ++i) {
      threads[i].join();
    }

    if (!earlyTermination) {
      for (int i = 0; i < numPartitions; ++i) {
        ASSERT_EQ(fetchedPackedColumns[i], producedPackedColumns[i]);
      }
    }
    queueManager_->removeTask(taskId);
  }
}

// Test BUG B scenario (a): Producer never starts / never calls
// initializeTask(). getData() creates a stub queue, then removeTask() is
// called without initializeTask() ever being called. The pending callback
// must fire with nullptr to unblock the consumer.
TEST_F(UcxOutputQueueManagerTest, callbackFiredOnTerminateBeforeInit) {
  const std::string taskId = "orphanTest";
  queueManager_->removeTask(taskId); // ensure clean state
  queueManager_->expectTask(
      taskId, 1, core::PartitionedOutputNode::Kind::kPartitioned);

  bool callbackFired = false;
  bool receivedNullptr = false;
  queueManager_->getData(
      taskId,
      0, // destination
      [&callbackFired, &receivedNullptr](
          std::shared_ptr<cudf::packed_columns> data,
          std::vector<int64_t> remainingBytes) {
        callbackFired = true;
        receivedNullptr = (data == nullptr);
      });

  // Callback should NOT have fired yet (queue is empty, no data).
  EXPECT_FALSE(callbackFired);

  // Now simulate the producer failing: removeTask fires terminate().
  // The stub queue has task_ == nullptr so the isRunning() check is skipped.
  queueManager_->removeTask(taskId);

  // Callback must have been fired with nullptr.
  EXPECT_TRUE(callbackFired);
  EXPECT_TRUE(receivedNullptr);
}

// Test BUG B scenario (b): Producer starts but crashes before noMoreData().
// initializeTask() was called but noMoreData() was never called.
// removeTask() must fire pending callbacks with nullptr.
TEST_F(UcxOutputQueueManagerTest, callbackFiredOnTerminateAfterInit) {
  const std::string taskId = "crashTest";
  queueManager_->removeTask(taskId); // ensure clean state

  auto task =
      initializeTask(taskId, 2 /* numDestinations */, 1 /* numDrivers */);

  // Register callbacks on both destinations.
  bool callback0Fired = false;
  bool callback0Nullptr = false;
  bool callback1Fired = false;
  bool callback1Nullptr = false;
  queueManager_->getData(
      taskId,
      0,
      [&callback0Fired, &callback0Nullptr](
          std::shared_ptr<cudf::packed_columns> data,
          std::vector<int64_t> remainingBytes) {
        callback0Fired = true;
        callback0Nullptr = (data == nullptr);
      });
  queueManager_->getData(
      taskId,
      1,
      [&callback1Fired, &callback1Nullptr](
          std::shared_ptr<cudf::packed_columns> data,
          std::vector<int64_t> remainingBytes) {
        callback1Fired = true;
        callback1Nullptr = (data == nullptr);
      });

  // Neither callback should fire yet.
  EXPECT_FALSE(callback0Fired);
  EXPECT_FALSE(callback1Fired);

  // Simulate task failure: abort the task so isRunning() returns false,
  // which is required by terminate()'s VELOX_CHECK.
  task->requestAbort().wait();

  queueManager_->removeTask(taskId);

  EXPECT_TRUE(callback0Fired);
  EXPECT_TRUE(callback0Nullptr);
  EXPECT_TRUE(callback1Fired);
  EXPECT_TRUE(callback1Nullptr);
}

// --- Broadcast tests ---

// A predeclared Spark contract can contain the final fanout before Velox
// incrementally exposes the first buffers. Intermediate counts must not be
// misclassified as an attempt to shrink the admitted contract.
TEST_F(UcxOutputQueueManagerTest, broadcastPredeclaredFinalFanout) {
  const std::string taskId = "broadcast-predeclared";
  queueManager_->removeTask(taskId);
  queueManager_->expectTask(
      taskId, 4, core::PartitionedOutputNode::Kind::kBroadcast);

  auto task = createSourceTask(taskId, pool_, UcxTestData::kTestRowType);
  queueManager_->initializeTask(
      task,
      core::PartitionedOutputNode::Kind::kBroadcast,
      1,
      1 /* numDrivers */);

  EXPECT_NO_THROW(queueManager_->updateOutputBuffers(taskId, 2, false));
  EXPECT_NO_THROW(queueManager_->updateOutputBuffers(taskId, 4, true));
  queueManager_->removeTask(taskId);
}

// Basic broadcast: enqueue data, all destinations receive the same data.
TEST_F(UcxOutputQueueManagerTest, broadcastBasic) {
  const vector_size_t size = 100;
  const std::string taskId = "broadcast0";
  const int numDestinations = 3;

  auto task = initializeTask(
      taskId,
      numDestinations,
      1 /* numDrivers */,
      true /* cleanup */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Broadcast must not be finished before noMoreQueues.
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  // Tell the queue manager the final number of destinations.
  queueManager_->updateOutputBuffers(taskId, numDestinations, true);

  // Enqueue one batch (broadcast always uses destination 0).
  enqueue(taskId, 0, size);

  // All destinations should receive the data.
  for (int dest = 0; dest < numDestinations; ++dest) {
    fetch(taskId, dest);
  }

  // Signal no more data.
  noMoreData(taskId);

  // Fetch end markers from all destinations.
  for (int dest = 0; dest < numDestinations; ++dest) {
    fetchEndMarker(taskId, dest);
  }

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: late destination receives backfilled data.
TEST_F(UcxOutputQueueManagerTest, broadcastLateDestination) {
  const vector_size_t size = 50;
  const std::string taskId = "broadcast1";

  // Start with 2 destinations.
  auto task = initializeTask(
      taskId,
      2 /* numDestinations */,
      1 /* numDrivers */,
      true /* cleanup */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Enqueue 2 batches before the 3rd destination arrives.
  enqueue(taskId, 0, size);
  enqueue(taskId, 0, size);

  // Fetch from existing destinations to verify data is there.
  fetch(taskId, 0);
  fetch(taskId, 1);

  // Now add a 3rd destination (late arrival). It should be backfilled.
  queueManager_->updateOutputBuffers(taskId, 3, false);

  // The new destination should have both batches.
  fetch(taskId, 2);
  fetch(taskId, 2);

  // Finalize destinations and data.
  queueManager_->updateOutputBuffers(taskId, 3, true);
  noMoreData(taskId);

  // Fetch remaining data + end markers.
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);
  fetch(taskId, 1);
  fetchEndMarker(taskId, 1);
  fetchEndMarker(taskId, 2);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: isFinished requires noMoreQueues.
TEST_F(UcxOutputQueueManagerTest, broadcastNotFinishedWithoutNoMoreQueues) {
  const std::string taskId = "broadcast2";

  auto task = initializeTask(
      taskId,
      2 /* numDestinations */,
      1 /* numDrivers */,
      true /* cleanup */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Signal no more data.
  noMoreData(taskId);

  // Fetch end markers and delete results from all destinations.
  fetchEndMarker(taskId, 0);
  fetchEndMarker(taskId, 1);

  // Still not finished because noMoreQueues hasn't been signaled.
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  // Signal no more queues.
  queueManager_->updateOutputBuffers(taskId, 2, true);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: end marker propagated to late-arriving destinations.
TEST_F(UcxOutputQueueManagerTest, broadcastEndMarkerToLateDestination) {
  const std::string taskId = "broadcast3";

  auto task = initializeTask(
      taskId,
      1 /* numDestinations */,
      1 /* numDrivers */,
      true /* cleanup */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  enqueue(taskId, 0, 10);
  noMoreData(taskId);

  // Add a late destination after end marker was set.
  queueManager_->updateOutputBuffers(taskId, 2, true);

  // Destination 0: data + end marker.
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);

  // Destination 1 (late): backfilled data + end marker.
  fetch(taskId, 1);
  fetchEndMarker(taskId, 1);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}
