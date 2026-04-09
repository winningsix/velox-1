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

#include "velox/experimental/cudf/exec/GpuGuard.h"

#include <cuda_runtime.h>

namespace facebook::velox::cudf_velox {

namespace {
thread_local bool gpuRegionActive = false;
} // namespace

void beginGpuRegion() {
  if (!gpuRegionActive) {
    gluten::lockGpu();
    gpuRegionActive = true;
  }
}

void endGpuRegion() {
  if (gpuRegionActive) {
    // Synchronize ALL CUDA streams before releasing the GPU semaphore.
    // Without this, async operations (cudaMemcpyAsync, kernel launches)
    // from the current thread may still be in-flight when the next thread
    // acquires the semaphore and starts new CUDA operations, causing
    // SEGV_ACCERR in the CUDA driver on concurrent access.
    // This is the root cause of the Blackwell SIGSEGV: v4 accidentally
    // serialized everything via semaphore leak; proper endGpuRegion
    // without sync creates async overlap between threads.
    cudaDeviceSynchronize();
    gpuRegionActive = false;
    gluten::unlockGpu();
  }
}

bool isInGpuRegion() {
  return gpuRegionActive;
}

} // namespace facebook::velox::cudf_velox
