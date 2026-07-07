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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace facebook::velox::ucx_exchange {
class UcxExchangeClient;
}

namespace facebook::velox::cudf_velox {

/// Process registry for the weak UCX clients created by exchange adapters.
/// Task-terminal cleanup removes every plan-node key deterministically; weak
/// ownership ensures the registry never extends a client's lifetime.
class UcxExchangeClientRegistry {
 public:
  using Client = ucx_exchange::UcxExchangeClient;
  using ClientPtr = std::shared_ptr<Client>;
  using Factory = std::function<ClientPtr()>;

  struct Lookup {
    ClientPtr client;
    bool created{false};
  };

  struct Stats {
    size_t activeEntries{0};
    size_t expiredEntries{0};
    uint64_t totalRegistered{0};
    uint64_t totalCleaned{0};
    uint64_t totalExpired{0};
    uint64_t totalCleanupCalls{0};
  };

  Lookup getOrCreate(
      const std::string& taskId,
      const std::string& planNodeId,
      const Factory& factory);

  ClientPtr find(
      const std::string& taskId,
      const std::string& planNodeId);

  void registerClient(
      const std::string& taskId,
      const std::string& planNodeId,
      const ClientPtr& client);

  /// Erases all weak keys for a terminal task. Idempotent and thread-safe.
  size_t cleanupTask(const std::string& taskId);

  Stats stats() const;

 private:
  struct Key {
    std::string taskId;
    std::string planNodeId;

    bool operator==(const Key& other) const {
      return taskId == other.taskId && planNodeId == other.planNodeId;
    }

    struct Hash {
      size_t operator()(const Key& key) const;
    };
  };

  void eraseExpiredLocked(
      std::unordered_map<Key, std::weak_ptr<Client>, Key::Hash>::iterator it);

  mutable std::mutex mutex_;
  std::unordered_map<Key, std::weak_ptr<Client>, Key::Hash> clients_;
  uint64_t totalRegistered_{0};
  uint64_t totalCleaned_{0};
  uint64_t totalExpired_{0};
  uint64_t totalCleanupCalls_{0};
};

UcxExchangeClientRegistry& ucxExchangeClientRegistry();

/// Terminal-task hook used by query coordinators. Returns the number of plan
/// node keys removed for observability and is safe to call repeatedly.
size_t cleanupUcxExchangeClientsForTask(const std::string& taskId);

UcxExchangeClientRegistry::Stats ucxExchangeClientRegistryStats();

} // namespace facebook::velox::cudf_velox
