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
#include "velox/experimental/ucx-exchange/EndpointRef.h"
#include <gtest/gtest.h>

using namespace facebook::velox::ucx_exchange;

namespace {

class DummyCommElement : public CommElement {
 public:
  DummyCommElement() : CommElement(nullptr) {}

  void process() override {}

  void close() override {}
};

std::shared_ptr<DummyCommElement> dummyCommElement() {
  return std::make_shared<DummyCommElement>();
}

} // namespace

TEST(EndpointRefTest, QueuesSendTransactionsInOrder) {
  auto endpointRef = std::make_shared<EndpointRef>(nullptr);
  auto first = dummyCommElement();
  auto second = dummyCommElement();
  auto third = dummyCommElement();

  ASSERT_TRUE(endpointRef->tryAcquireSendTransaction(first));
  ASSERT_FALSE(endpointRef->tryAcquireSendTransaction(second));
  ASSERT_FALSE(endpointRef->tryAcquireSendTransaction(third));
  ASSERT_FALSE(endpointRef->tryAcquireSendTransaction(second));

  ASSERT_EQ(endpointRef->releaseSendTransaction(), second);
  ASSERT_TRUE(endpointRef->tryAcquireSendTransaction(second));

  ASSERT_EQ(endpointRef->releaseSendTransaction(), third);
  ASSERT_TRUE(endpointRef->tryAcquireSendTransaction(third));

  ASSERT_EQ(endpointRef->releaseSendTransaction(), nullptr);
}

TEST(EndpointRefTest, CancelQueuedWaiterWakesNextIdleWaiter) {
  auto endpointRef = std::make_shared<EndpointRef>(nullptr);
  auto first = dummyCommElement();
  auto second = dummyCommElement();
  auto third = dummyCommElement();

  ASSERT_TRUE(endpointRef->tryAcquireSendTransaction(first));
  ASSERT_FALSE(endpointRef->tryAcquireSendTransaction(second));
  ASSERT_FALSE(endpointRef->tryAcquireSendTransaction(third));

  ASSERT_EQ(endpointRef->releaseSendTransaction(), second);
  ASSERT_EQ(endpointRef->cancelSendTransactionWaiter(second), third);
  ASSERT_TRUE(endpointRef->tryAcquireSendTransaction(third));

  ASSERT_EQ(endpointRef->releaseSendTransaction(), nullptr);
}
