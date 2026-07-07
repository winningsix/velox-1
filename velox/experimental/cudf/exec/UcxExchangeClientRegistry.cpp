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

#include <stdexcept>
#include <utility>

namespace facebook::velox::cudf_velox {

size_t UcxExchangeClientRegistry::Key::Hash::operator()(
    const Key& key) const {
  return std::hash<std::string>{}(key.taskId) ^
      (std::hash<std::string>{}(key.planNodeId) << 1);
}

void UcxExchangeClientRegistry::eraseExpiredLocked(
    decltype(clients_)::iterator it) {
  clients_.erase(it);
  ++totalExpired_;
  ++totalCleaned_;
}

UcxExchangeClientRegistry::Lookup UcxExchangeClientRegistry::getOrCreate(
    const std::string& taskId,
    const std::string& planNodeId,
    const Factory& factory) {
  std::lock_guard<std::mutex> lock(mutex_);
  const Key key{taskId, planNodeId};
  auto it = clients_.find(key);
  if (it != clients_.end()) {
    if (auto client = it->second.lock()) {
      return {std::move(client), false};
    }
    eraseExpiredLocked(it);
  }
  if (!factory) {
    throw std::invalid_argument("UCX exchange client factory is null");
  }
  auto client = factory();
  if (client == nullptr) {
    throw std::invalid_argument("UCX exchange client factory returned null");
  }
  clients_.emplace(key, client);
  ++totalRegistered_;
  return {std::move(client), true};
}

UcxExchangeClientRegistry::ClientPtr UcxExchangeClientRegistry::find(
    const std::string& taskId,
    const std::string& planNodeId) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = clients_.find(Key{taskId, planNodeId});
  if (it == clients_.end()) {
    return nullptr;
  }
  auto client = it->second.lock();
  if (client == nullptr) {
    eraseExpiredLocked(it);
  }
  return client;
}

void UcxExchangeClientRegistry::registerClient(
    const std::string& taskId,
    const std::string& planNodeId,
    const ClientPtr& client) {
  if (client == nullptr) {
    throw std::invalid_argument("Cannot register a null UCX exchange client");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const Key key{taskId, planNodeId};
  auto it = clients_.find(key);
  if (it != clients_.end()) {
    if (it->second.expired()) {
      ++totalExpired_;
    }
    clients_.erase(it);
    ++totalCleaned_;
  }
  clients_.emplace(key, client);
  ++totalRegistered_;
}

size_t UcxExchangeClientRegistry::cleanupTask(const std::string& taskId) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++totalCleanupCalls_;
  size_t removed = 0;
  for (auto it = clients_.begin(); it != clients_.end();) {
    if (it->first.taskId != taskId) {
      ++it;
      continue;
    }
    if (it->second.expired()) {
      ++totalExpired_;
    }
    it = clients_.erase(it);
    ++removed;
  }
  totalCleaned_ += removed;
  return removed;
}

UcxExchangeClientRegistry::Stats UcxExchangeClientRegistry::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  Stats result;
  for (const auto& [key, client] : clients_) {
    if (client.expired()) {
      ++result.expiredEntries;
    } else {
      ++result.activeEntries;
    }
  }
  result.totalRegistered = totalRegistered_;
  result.totalCleaned = totalCleaned_;
  result.totalExpired = totalExpired_;
  result.totalCleanupCalls = totalCleanupCalls_;
  return result;
}

UcxExchangeClientRegistry& ucxExchangeClientRegistry() {
  // Process-lifetime avoids static teardown races with Task destruction.
  static auto* registry = new UcxExchangeClientRegistry();
  return *registry;
}

size_t cleanupUcxExchangeClientsForTask(const std::string& taskId) {
  return ucxExchangeClientRegistry().cleanupTask(taskId);
}

UcxExchangeClientRegistry::Stats ucxExchangeClientRegistryStats() {
  return ucxExchangeClientRegistry().stats();
}

} // namespace facebook::velox::cudf_velox
