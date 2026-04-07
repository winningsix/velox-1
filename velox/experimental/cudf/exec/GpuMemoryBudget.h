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

// Lock-free GPU memory admission control using RMM statistics.
// Replaces GpuGuard semaphore for Phase 2 async operator pattern.
// See plan/semaphore.md §3.6.2 and §3.6.6 for design rationale.

#pragma once

#include <rmm/mr/device_memory_resource.hpp>
#include <rmm/mr/statistics_resource_adaptor.hpp>

#include <atomic>
#include <cstdint>

namespace facebook::velox::cudf_velox {

/// Executor-level singleton for lock-free GPU memory admission control.
///
/// Replaces GpuGuard (semaphore-based lock) with a CAS-based admission gate
/// that uses real RMM pool statistics rather than synthetic counters.
///
/// Usage:
///   - Call init() once at executor startup after the RMM pool is configured.
///   - Call tryAdmit(estimatedBytes) from needsInput() — non-blocking.
///   - Call release(estimatedBytes) from CUDA stream callback when output consumed.
///
/// Thread safety: all public methods are safe to call concurrently.
class GpuMemoryBudget {
 public:
  static GpuMemoryBudget& instance();

  /// Initialize with the RMM statistics adaptor and pool capacity.
  /// Must be called once at executor startup, after RMM pool is configured.
  /// @param statsMr  Pointer to a statistics_resource_adaptor wrapping the
  ///                 device pool. Must remain alive for the executor lifetime.
  /// @param poolCapacityBytes  Total capacity of the RMM pool (bytes).
  void init(
      rmm::mr::statistics_resource_adaptor<rmm::mr::device_memory_resource>*
          statsMr,
      int64_t poolCapacityBytes);

  /// Try to admit estimatedBytes × kExpansionFactor into the GPU pipeline.
  /// Non-blocking — returns true if admission is granted (in-flight counter
  /// is updated via CAS). Returns false immediately if GPU memory is tight.
  ///
  /// Called from needsInput() so that Velox will re-poll on the next driver
  /// turn without blocking any thread.
  bool tryAdmit(int64_t estimatedBytes);

  /// Release the reservation made by a matching tryAdmit() call.
  /// Must be called once for every successful tryAdmit(), typically from a
  /// CUDA stream callback when the operator's GPU output has been consumed.
  void release(int64_t estimatedBytes);

  /// Return current pool utilization (actual RMM + in-flight), in bytes.
  /// For monitoring and logging only.
  int64_t currentUsageBytes() const;

  /// Reset all state — for unit testing only.
  void reset();

 private:
  GpuMemoryBudget() = default;

  // kExpansionFactor: conservative multiplier for output amplification and
  // cuDF internal temporaries (gather maps, sort keys, etc.).
  // Scan/filter operators: ~4× input. Joins may need higher but fall back to
  // grace join on OOM, so we keep this at 4 for all operators.
  static constexpr int64_t kExpansionFactor = 4;

  // Hard floor: always keep 512 MB free to prevent GPU OOM from background
  // allocations and cuDF internal buffers not visible to the caller.
  static constexpr int64_t kMinFreeBufferBytes = 512LL << 20; // 512 MB

  // Actual RMM pool statistics — O(1) read, thread-safe.
  // Null until init() is called; null means "allow all" (backward compat).
  rmm::mr::statistics_resource_adaptor<rmm::mr::device_memory_resource>*
      statsMr_{nullptr};

  // Total capacity of the RMM pool, set by init().
  int64_t poolCapacityBytes_{0};

  // Logical in-flight bytes: sum of all active tryAdmit() reservations.
  // CAS in tryAdmit() prevents multiple concurrent callers from all seeing
  // "budget OK" and then racing to admit more than the pool can hold.
  std::atomic<int64_t> inFlight_{0};
};

} // namespace facebook::velox::cudf_velox
