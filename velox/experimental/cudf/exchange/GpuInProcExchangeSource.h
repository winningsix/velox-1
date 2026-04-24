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

#include "velox/exec/ExchangeSource.h"

namespace facebook::velox::cudf_velox {

/// Factory for ExchangeSources whose remote task ID starts with
/// "gpu-inproc://". Returns nullptr for other prefixes so the factory chain
/// can fall through. Pairs with InProcessChannel on the producer side.
///
/// Unlike LocalGpuExchangeSource (which reads from OutputBufferManager +
/// wraps pages as SharedSerializedPage), this source reads CudfVectors
/// directly out of InProcessChannel and wraps each one in a
/// GpuSerializedPage at the boundary so the standard Velox Exchange
/// operator can hand it to GpuExchange, preserving zero-copy.
std::shared_ptr<exec::ExchangeSource> createGpuInProcExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool);

} // namespace facebook::velox::cudf_velox
