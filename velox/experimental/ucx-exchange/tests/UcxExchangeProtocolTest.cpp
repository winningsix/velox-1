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

#include "velox/experimental/ucx-exchange/UcxExchangeProtocol.h"
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"

#include <array>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

namespace facebook::velox::ucx_exchange {
namespace {

TEST(UcxExchangeProtocolTest, TaskIdWireBoundary) {
  EXPECT_FALSE(isValidHandshakeTaskId(""));
  EXPECT_TRUE(
      isValidHandshakeTaskId(std::string(kUcxHandshakeMaxTaskIdLength, 'a')));
  EXPECT_FALSE(
      isValidHandshakeTaskId(std::string(kUcxHandshakeTaskIdCapacity, 'a')));

  const std::string embeddedNull{"valid\0truncated", 15};
  EXPECT_FALSE(isValidHandshakeTaskId(embeddedNull));

  HandshakeMsg canonical;
  std::memset(canonical.taskId, 'a', kUcxHandshakeMaxTaskIdLength);
  canonical.taskId[kUcxHandshakeMaxTaskIdLength] = '\0';
  EXPECT_TRUE(isCanonicalHandshakeTaskIdBuffer(
      canonical.taskId, sizeof(canonical.taskId)));

  HandshakeMsg empty;
  EXPECT_FALSE(
      isCanonicalHandshakeTaskIdBuffer(empty.taskId, sizeof(empty.taskId)));

  HandshakeMsg unterminated;
  std::memset(unterminated.taskId, 'a', kUcxHandshakeTaskIdCapacity);
  EXPECT_FALSE(isCanonicalHandshakeTaskIdBuffer(
      unterminated.taskId, sizeof(unterminated.taskId)));

  HandshakeMsg ambiguous;
  std::memcpy(ambiguous.taskId, "valid", 5);
  ambiguous.taskId[5] = '\0';
  ambiguous.taskId[6] = 'x';
  EXPECT_FALSE(isCanonicalHandshakeTaskIdBuffer(
      ambiguous.taskId, sizeof(ambiguous.taskId)));
}

TEST(UcxExchangeProtocolTest, EveryAdmissionFailureHasExplicitWireStatus) {
  constexpr std::array<HandshakeStatus, 7> kRejectedStatuses = {
      HandshakeStatus::kInvalidRequest,
      HandshakeStatus::kInvalidDestination,
      HandshakeStatus::kDuplicateRequest,
      HandshakeStatus::kAdmissionCapacity,
      HandshakeStatus::kAdmissionExpired,
      HandshakeStatus::kTaskRetired,
      HandshakeStatus::kShuttingDown};

  for (const auto status : kRejectedStatuses) {
    SCOPED_TRACE(static_cast<uint32_t>(status));
    EXPECT_NE(status, HandshakeStatus::kAccepted);
    EXPECT_NE(handshakeStatusName(status), "UNKNOWN");
  }
}

TEST(UcxExchangeProtocolTest, HandshakeFailureIsAnErrorNotEndOfStream) {
  auto client = std::make_shared<UcxExchangeClient>("consumer", 0, 1);
  client->queue()->setError("UCX handshake rejected: ADMISSION_CAPACITY");

  bool atEnd = false;
  ContinueFuture future;
  EXPECT_THROW(client->next(0, &atEnd, &future), VeloxRuntimeError);
  EXPECT_TRUE(atEnd);
}

} // namespace
} // namespace facebook::velox::ucx_exchange
