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

namespace gluten {
void lockGpu();
void unlockGpu();
} // namespace gluten

namespace facebook::velox::cudf_velox {

/// RAII guard that limits concurrent GPU usage across Velox pipeline tasks.
/// Forwards to gluten::lockGpu / unlockGpu which are ref-counted per thread.
struct GpuGuard {
  GpuGuard() {
    gluten::lockGpu();
  }
  ~GpuGuard() {
    gluten::unlockGpu();
  }
  GpuGuard(const GpuGuard&) = delete;
  GpuGuard& operator=(const GpuGuard&) = delete;
};

/// Pipeline-level GPU region using thread-local state.
///
/// A GPU region brackets a contiguous sequence of GPU work across multiple
/// operators within one pipeline iteration. It exploits the existing
/// thread-local ref-counting in lockGpu/unlockGpu: beginGpuRegion() bumps
/// the refcount, so all nested GpuGuard acquisitions become free (refcount
/// 1→2→1→2→1). The actual semaphore is never released between operators.
///
/// Usage pattern:
///   Source operator (after I/O, before H2D): beginGpuRegion()
///   Intermediate operators: GpuGuard as usual (nested, no-op on semaphore)
///   Sink / D2H point: endGpuRegion()
///
/// This gives the Spark-Rapids scope: [H2D acquire, D2H release] without
/// holding the lock during I/O at either end.
void beginGpuRegion();
void endGpuRegion();
bool isInGpuRegion();

} // namespace facebook::velox::cudf_velox
