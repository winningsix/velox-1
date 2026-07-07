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
#include <string>

namespace facebook::velox::ucx_exchange {

/// Identifies one live incarnation of a producer task. Task IDs may be reused
/// after retirement; epoch zero is reserved for an invalid/unbound token.
struct TaskToken {
  std::string taskId;
  uint64_t epoch{0};

  bool operator==(const TaskToken& other) const {
    return taskId == other.taskId && epoch == other.epoch;
  }

  bool operator!=(const TaskToken& other) const {
    return !(*this == other);
  }

  bool operator<(const TaskToken& other) const {
    if (taskId != other.taskId) {
      return taskId < other.taskId;
    }
    return epoch < other.epoch;
  }

  explicit operator bool() const {
    return !taskId.empty() && epoch != 0;
  }
};

} // namespace facebook::velox::ucx_exchange
