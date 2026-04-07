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

#include <cstdint>

namespace facebook::velox::cudf_velox {

/// Lock-free GPU memory budget tracker used to provide back-pressure in the
/// async GPU operator pattern (§3.6.3 of the semaphore redesign document).
///
/// This is a minimal stub.  The full implementation is provided by the
/// ferd/p2-gpu-memory-budget work item and will replace this file.
class GpuMemoryBudget {
 public:
  static GpuMemoryBudget& instance() {
    static GpuMemoryBudget budget;
    return budget;
  }

  /// Attempt to reserve `bytes` of GPU memory from the budget.
  /// Returns true if the reservation succeeded, false if the budget is
  /// exhausted and the caller should back off.
  bool tryReserve(int64_t /*bytes*/) {
    return true;
  }

  /// Release a previously reserved `bytes` from the budget.
  void release(int64_t /*bytes*/) {}

 private:
  GpuMemoryBudget() = default;
};

} // namespace facebook::velox::cudf_velox
