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

#include "velox/experimental/cudf/exec/GpuMemoryBudget.h"

namespace facebook::velox::cudf_velox {

GpuMemoryBudget& GpuMemoryBudget::instance() {
  static GpuMemoryBudget budget;
  return budget;
}

void GpuMemoryBudget::init(
    rmm::mr::statistics_resource_adaptor<rmm::mr::device_memory_resource>*
        statsMr,
    int64_t poolCapacityBytes) {
  statsMr_ = statsMr;
  poolCapacityBytes_ = poolCapacityBytes;
  inFlight_.store(0, std::memory_order_release);
}

bool GpuMemoryBudget::tryAdmit(int64_t estimatedBytes) {
  if (statsMr_ == nullptr || poolCapacityBytes_ == 0) {
    // Not yet initialized — allow all for backward compatibility.
    return true;
  }

  int64_t required = estimatedBytes * kExpansionFactor + kMinFreeBufferBytes;

  // Query actual RMM pool usage — O(1), thread-safe via shared_mutex inside
  // statistics_resource_adaptor. counter::value is current allocated bytes.
  auto stats = statsMr_->get_bytes_counter();
  int64_t actualUsed = stats.value;
  int64_t actualFree = poolCapacityBytes_ - actualUsed;

  // Fast path: reject immediately if actual pool headroom is insufficient.
  if (actualFree < required) {
    return false;
  }

  // CAS loop: prevents multiple concurrent needsInput() callers from all
  // seeing "free OK" and admitting simultaneous batches that together would
  // overflow the pool. The in-flight counter is a secondary guard on top of
  // the actual-usage check above.
  int64_t prev = inFlight_.load(std::memory_order_acquire);
  while (prev + required <= poolCapacityBytes_) {
    if (inFlight_.compare_exchange_weak(
            prev,
            prev + required,
            std::memory_order_release,
            std::memory_order_relaxed)) {
      return true;
    }
    // compare_exchange_weak updates prev on failure; loop retries.
  }
  return false;
}

void GpuMemoryBudget::release(int64_t estimatedBytes) {
  int64_t required = estimatedBytes * kExpansionFactor + kMinFreeBufferBytes;
  inFlight_.fetch_sub(required, std::memory_order_release);
}

int64_t GpuMemoryBudget::currentUsageBytes() const {
  if (statsMr_ == nullptr) {
    return inFlight_.load(std::memory_order_acquire);
  }
  int64_t actualUsed = statsMr_->get_bytes_counter().value;
  int64_t inFlight = inFlight_.load(std::memory_order_acquire);
  return actualUsed + inFlight;
}

void GpuMemoryBudget::reset() {
  statsMr_ = nullptr;
  poolCapacityBytes_ = 0;
  inFlight_.store(0, std::memory_order_release);
}

} // namespace facebook::velox::cudf_velox
