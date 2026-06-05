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
#include <folly/Synchronized.h>
#include <ucxx/api.h>
#include <ucxx/utils/ucx.h>
#include <velox/exec/Task.h>
#include <velox/experimental/ucx-exchange/UcxOutputQueueManager.h>
#include <chrono>
#include <future>
#include <memory>
#include <tuple>
#include "velox/experimental/ucx-exchange/CommElement.h"
#include "velox/experimental/ucx-exchange/EndpointRef.h"
#include "velox/experimental/ucx-exchange/PartitionKey.h"

namespace facebook::velox::ucx_exchange {

class UcxExchangeServer
    : public CommElement,
      public std::enable_shared_from_this<UcxExchangeServer> {
 public:
  /// @brief Factory method to create a UcxExchangeServer.
  /// @param communicator The Communicator instance.
  /// @param endpointRef The endpoint reference for UCXX communication.
  /// @param key The partition key identifying the data to serve.
  static std::shared_ptr<UcxExchangeServer> create(
      const std::shared_ptr<Communicator> communicator,
      std::shared_ptr<EndpointRef> endpointRef,
      const PartitionKey& key);

  void process() override;

  void close() override;

  std::string toString();

  const PartitionKey& getPartitionKey() const {
    return partitionKey_;
  }

 private:
  enum class ServerState : uint32_t {
    Created,
    ReadyToTransfer,
    DataRequestReady,
    WaitingForDataFromQueue,
    DataReady,
    WaitingForSendComplete,
    Done
  };

  explicit UcxExchangeServer(
      const std::shared_ptr<Communicator> communicator,
      std::shared_ptr<EndpointRef> endpointRef,
      const PartitionKey& key);

  /// @return A shared pointer to itself.
  std::shared_ptr<UcxExchangeServer> getSelfPtr();

  /// @brief Sends metadata and data to the connected receiver.
  void sendData();

  /// @brief Completion handler after data has been sent.
  void sendComplete(ucs_status_t status, std::shared_ptr<void> arg);

  /// @brief Sets the new state of this exchange server using
  /// sequential consistency. Logs transitions at VLOG(2).
  /// @param newState the new state of the UcxExchangeServer.
  void setState(ServerState newState);

  /// @brief Returns the state.
  ServerState getState() {
    return state_.load(std::memory_order_seq_cst);
  }

  static std::string getStateAsString(ServerState s) {
    const std::string stateMap[] = {
        "Created",
        "ReadyToTransfer",
        "DataRequestReady",
        "WaitingForDataFromQueue",
        "DataReady",
        "WaitingForSendComplete",
        "Done"};
    return stateMap[static_cast<uint32_t>(s)];
  }

  std::string getStateAsString() {
    return getStateAsString(state_.load());
  }

  const PartitionKey partitionKey_;
  const uint32_t
      partitionKeyHash_; // A hash of above, used to create unique tags.

  std::atomic<ServerState> state_;
  std::shared_ptr<cudf::packed_columns> dataPtr_{nullptr};
  std::recursive_mutex dataMutex_; // mutex for above ptr.
  std::atomic<bool> closed_{false};

  uint32_t sequenceNumber_{0};

  /// Whether this server currently holds a data-send slot on its endpoint.
  /// Send-slot concurrency control (via EndpointRef::tryAcquireDataSendSlot),
  /// independent of flow control.
  std::atomic<bool> dataSendSlotAcquired_{false};

  // The outstanding requests - there can only be one outstanding request
  // of each type at any point in time.
  // NOTE: The request owns/holds references to the upcall function
  // and must therefore exist until the upcall is done.
  std::shared_ptr<ucxx::Request> metaRequest_{nullptr};
  std::shared_ptr<ucxx::Request> dataRequest_{nullptr};

  // Completed UCXX requests are kept alive here to prevent use-after-free.
  // UCP's ucp_wireup_replay_pending_requests can fire callbacks on already-
  // completed requests; if the ucxx::Request has been freed, the callback
  // lambda is in freed memory and crashes. Retaining them here ensures the
  // Request (and its callback lambda) stays valid for the lifetime of this
  // server.
  std::vector<std::shared_ptr<ucxx::Request>> completedRequests_;

  std::chrono::time_point<std::chrono::high_resolution_clock> sendStart_;
  std::size_t bytes_;

  std::shared_ptr<UcxOutputQueueManager> queueMgr_;
};

} // namespace facebook::velox::ucx_exchange
