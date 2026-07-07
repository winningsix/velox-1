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
#include "velox/experimental/ucx-exchange/Acceptor.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/EndpointRef.h"
#include "velox/experimental/ucx-exchange/UcxExchangeProtocol.h"
#include "velox/experimental/ucx-exchange/UcxExchangeServer.h"
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <mutex>
#include <vector>

namespace facebook::velox::ucx_exchange {
namespace {

std::atomic<bool> gHandshakeRespondersPausedForTest{false};
std::atomic<size_t> gActiveHandshakeResponders{0};
std::atomic<size_t> gPeakHandshakeResponders{0};
std::mutex gPausedHandshakeRespondersMutex;
std::vector<std::function<void()>> gPausedHandshakeResponderWakeups;

void incrementHandshakeResponders() {
  const auto active =
      gActiveHandshakeResponders.fetch_add(1, std::memory_order_acq_rel) + 1;
  auto peak = gPeakHandshakeResponders.load(std::memory_order_acquire);
  while (active > peak &&
         !gPeakHandshakeResponders.compare_exchange_weak(
             peak, active, std::memory_order_acq_rel)) {
  }
}

HandshakeStatus protocolStatus(
    UcxTaskLifecycleRegistry::AdmissionRejectReason reason) {
  switch (reason) {
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kNone:
      return HandshakeStatus::kAccepted;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kInvalidDestination:
      return HandshakeStatus::kInvalidDestination;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kDuplicate:
      return HandshakeStatus::kDuplicateRequest;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kCapacity:
      return HandshakeStatus::kAdmissionCapacity;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kExpired:
      return HandshakeStatus::kAdmissionExpired;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kRetired:
      return HandshakeStatus::kTaskRetired;
    case UcxTaskLifecycleRegistry::AdmissionRejectReason::kShutdown:
      return HandshakeStatus::kShuttingDown;
  }
  return HandshakeStatus::kInvalidRequest;
}

void sendHandshakeResponse(
    const std::shared_ptr<EndpointRef>& endpointRef,
    const PartitionKey& key,
    HandshakeStatus status,
    bool isIntraNodeTransfer,
    uint32_t destinationCount,
    std::function<void()> onFailure = {}) {
  auto response = std::make_shared<HandshakeResponse>();
  response->protocolVersion = kUcxExchangeProtocolVersion;
  response->status = status;
  response->isIntraNodeTransfer =
      status == HandshakeStatus::kAccepted && isIntraNodeTransfer;
  response->destinationCount = destinationCount;

  const auto responseTag = getHandshakeResponseTag(fnv1a_32(key.toString()));
  endpointRef->endpoint_->tagSend(
      response.get(),
      sizeof(*response),
      ucxx::Tag{responseTag},
      false,
      [response, keyString = key.toString(), onFailure = std::move(onFailure)](
          ucs_status_t sendStatus, std::shared_ptr<void>) {
        if (sendStatus != UCS_OK) {
          LOG(ERROR) << "Failed to send UCX handshake response for "
                     << keyString << ": " << ucs_status_string(sendStatus);
          if (onFailure) {
            onFailure();
          }
        }
      },
      response);
}

class PendingHandshake final
    : public CommElement,
      public std::enable_shared_from_this<PendingHandshake> {
 public:
  ~PendingHandshake() override {
    uncountResponder();
  }

  static std::shared_ptr<PendingHandshake> create(
      const std::shared_ptr<Communicator>& communicator,
      const std::shared_ptr<EndpointRef>& endpointRef,
      PartitionKey key,
      uint64_t sourceWorkerId) {
    return std::shared_ptr<PendingHandshake>(new PendingHandshake(
        communicator, endpointRef, std::move(key), sourceWorkerId));
  }

  void accept(
      UcxTaskLifecycleRegistry::TaskContract contract,
      std::shared_ptr<UcxOutputQueueManager::HandshakeReservation>
          reservation) {
    VELOX_CHECK_NOT_NULL(reservation);
    resolve(HandshakeStatus::kAccepted, contract, std::move(reservation));
  }

  void reject(HandshakeStatus status) {
    VELOX_CHECK_NE(
        static_cast<uint32_t>(status),
        static_cast<uint32_t>(HandshakeStatus::kAccepted));
    resolve(status, {}, nullptr);
  }

  void process() override {
    HandshakeStatus status;
    UcxTaskLifecycleRegistry::TaskContract contract;
    std::shared_ptr<UcxOutputQueueManager::HandshakeReservation> reservation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (decision_ == Decision::kPending || processed_) {
        return;
      }
      if (parkIfPausedForTest()) {
        return;
      }
      processed_ = true;
      status = status_;
      contract = contract_;
      reservation = reservation_;
    }

    bool isIntraNodeTransfer = false;
    std::shared_ptr<UcxExchangeServer> exchangeServer;
    if (status == HandshakeStatus::kAccepted) {
      try {
        auto queueMgr = UcxOutputQueueManager::getInstanceRef();
        const auto currentContract = queueMgr->taskContract(key_.taskId);
        if (!currentContract.has_value() ||
            currentContract->outputKind != contract.outputKind ||
            currentContract->destinationCount < contract.destinationCount ||
            key_.destination >= currentContract->destinationCount) {
          status = HandshakeStatus::kTaskRetired;
          reservation.reset();
          reservation_.reset();
        } else {
          // A broadcast contract may expand while this accepted response is
          // queued. Report the current monotonic bound rather than treating a
          // safe expansion as task replacement.
          contract = *currentContract;
        }
      } catch (...) {
        status = HandshakeStatus::kTaskRetired;
        reservation.reset();
        reservation_.reset();
      }
    }
    if (status == HandshakeStatus::kAccepted) {
      try {
        auto queueMgr = UcxOutputQueueManager::getInstanceRef();
        const bool sameWorker = sourceWorkerId_ == communicator_->getWorkerId();
        isIntraNodeTransfer =
            cudf_velox::CudfConfig::getInstance().intraNodeExchange &&
            sameWorker && queueMgr->canUseIntraNode(key_.taskId);

        exchangeServer = UcxExchangeServer::create(
            communicator_,
            endpointRef_,
            key_,
            isIntraNodeTransfer,
            reservation);
        if (!endpointRef_->addCommElem(exchangeServer)) {
          status = HandshakeStatus::kShuttingDown;
          isIntraNodeTransfer = false;
        } else {
          try {
            communicator_->registerCommElement(exchangeServer);
            reservation_.reset();
          } catch (...) {
            endpointRef_->removeCommElem(exchangeServer);
            communicator_->unregister(exchangeServer);
            exchangeServer.reset();
            throw;
          }
        }
      } catch (const std::exception& error) {
        LOG(ERROR) << "Failed to create admitted UCX exchange server for "
                   << key_.toString() << ": " << error.what();
        status = HandshakeStatus::kInvalidRequest;
        isIntraNodeTransfer = false;
      } catch (...) {
        LOG(ERROR) << "Failed to create admitted UCX exchange server for "
                   << key_.toString();
        status = HandshakeStatus::kInvalidRequest;
        isIntraNodeTransfer = false;
      }
    }

    try {
      std::weak_ptr<UcxExchangeServer> weakServer = exchangeServer;
      sendHandshakeResponse(
          endpointRef_,
          key_,
          status,
          isIntraNodeTransfer,
          contract.destinationCount,
          [weakServer]() {
            if (auto server = weakServer.lock()) {
              server->close();
            }
          });
    } catch (const std::exception& error) {
      LOG(ERROR) << "Failed to enqueue UCX handshake response for "
                 << key_.toString() << ": " << error.what();
      if (exchangeServer) {
        exchangeServer->close();
      }
    } catch (...) {
      LOG(ERROR) << "Failed to enqueue UCX handshake response for "
                 << key_.toString();
      if (exchangeServer) {
        exchangeServer->close();
      }
    }
    reservation_.reset();
    finish();
  }

  void close() override {
    std::shared_ptr<EndpointRef> endpoint;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (processed_) {
        return;
      }
      processed_ = true;
      decision_ = Decision::kResolved;
      status_ = HandshakeStatus::kShuttingDown;
      reservation_.reset();
      endpoint = endpointRef_;
    }
    auto self = shared_from_this();
    if (endpoint) {
      endpoint->removeCommElem(self);
    }
    uncountResponder();
    communicator_->unregister(self);
  }

 private:
  enum class Decision : uint8_t { kPending, kResolved };

  PendingHandshake(
      const std::shared_ptr<Communicator>& communicator,
      const std::shared_ptr<EndpointRef>& endpointRef,
      PartitionKey key,
      uint64_t sourceWorkerId)
      : CommElement(communicator, endpointRef),
        key_(std::move(key)),
        sourceWorkerId_(sourceWorkerId) {}

  void resolve(
      HandshakeStatus status,
      UcxTaskLifecycleRegistry::TaskContract contract,
      std::shared_ptr<UcxOutputQueueManager::HandshakeReservation>
          reservation) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (decision_ != Decision::kPending || processed_) {
        return;
      }
      decision_ = Decision::kResolved;
      status_ = status;
      contract_ = contract;
      reservation_ = std::move(reservation);
    }

    // Admission callbacks can run on the task thread, the lifecycle reaper,
    // or a shutdown thread. They must not construct an exchange server or
    // issue UCX operations. Registering this resolved responder is the only
    // cross-thread action; process() performs all UCX work on the Communicator
    // thread. No endpoint element or work item exists before this point.
    auto self = shared_from_this();
    bool registered = false;
    bool attached = false;
    try {
      communicator_->registerCommElement(self, false);
      registered = true;
      countedResponder_.store(true, std::memory_order_release);
      incrementHandshakeResponders();
      attached = endpointRef_ && endpointRef_->addCommElem(self);
      if (!attached) {
        // Keep the one-shot responder alive and marshal an explicit reject to
        // the progress thread. No exchange server is created on this path.
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = HandshakeStatus::kShuttingDown;
        reservation_.reset();
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        // Endpoint teardown can call close() after addCommElem() and before
        // this point. In that case close() already unregistered us.
        if (processed_) {
          return;
        }
      }
      communicator_->addToWorkQueue(self);
    } catch (const std::exception& error) {
      LOG(ERROR) << "Failed to register admitted UCX handshake responder for "
                 << key_.toString() << ": " << error.what();
      if (attached && endpointRef_) {
        endpointRef_->removeCommElem(self);
      }
      if (registered) {
        communicator_->unregister(self);
      }
      scheduleExplicitRegistrationFailure(self);
    } catch (...) {
      LOG(ERROR) << "Failed to register admitted UCX handshake responder for "
                 << key_.toString();
      if (attached && endpointRef_) {
        endpointRef_->removeCommElem(self);
      }
      if (registered) {
        communicator_->unregister(self);
      }
      scheduleExplicitRegistrationFailure(self);
    }
  }

  void scheduleExplicitRegistrationFailure(
      const std::shared_ptr<PendingHandshake>& self) noexcept {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (processed_) {
        return;
      }
      status_ = HandshakeStatus::kInvalidRequest;
      reservation_.reset();
    }
    // WorkQueue itself owns the responder until process(). This fallback is
    // intentionally independent of the communicator registry, whose insertion
    // just failed. All UCX work still happens on the progress thread.
    try {
      communicator_->addToWorkQueue(self);
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      processed_ = true;
    }
  }

  bool parkIfPausedForTest() {
    if (!gHandshakeRespondersPausedForTest.load(std::memory_order_acquire)) {
      return false;
    }
    std::lock_guard<std::mutex> pauseLock(gPausedHandshakeRespondersMutex);
    // Synchronize with testingSetHandshakeRespondersPaused(false): if resume
    // won the race, continue processing instead of parking without a wakeup.
    if (!gHandshakeRespondersPausedForTest.load(std::memory_order_acquire)) {
      return false;
    }
    if (!parkedForTest_) {
      parkedForTest_ = true;
      std::weak_ptr<PendingHandshake> weak = weak_from_this();
      gPausedHandshakeResponderWakeups.emplace_back([weak]() {
        if (auto responder = weak.lock()) {
          responder->resumeFromTestPause();
        }
      });
    }
    return true;
  }

  void resumeFromTestPause() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      parkedForTest_ = false;
      if (processed_) {
        return;
      }
    }
    communicator_->addToWorkQueue(shared_from_this());
  }

  void finish() {
    auto self = shared_from_this();
    reservation_.reset();
    if (endpointRef_) {
      endpointRef_->removeCommElem(self);
    }
    uncountResponder();
    communicator_->unregister(self);
    endpointRef_.reset();
  }

  void uncountResponder() noexcept {
    if (countedResponder_.exchange(false, std::memory_order_acq_rel)) {
      gActiveHandshakeResponders.fetch_sub(1, std::memory_order_acq_rel);
    }
  }

  const PartitionKey key_;
  const uint64_t sourceWorkerId_;
  std::mutex mutex_;
  Decision decision_{Decision::kPending};
  bool processed_{false};
  bool parkedForTest_{false};
  HandshakeStatus status_{HandshakeStatus::kInvalidRequest};
  UcxTaskLifecycleRegistry::TaskContract contract_;
  std::shared_ptr<UcxOutputQueueManager::HandshakeReservation> reservation_;
  std::atomic<bool> countedResponder_{false};
};

} // namespace

void Acceptor::testingSetHandshakeRespondersPaused(bool paused) {
  std::vector<std::function<void()>> wakeups;
  {
    std::lock_guard<std::mutex> lock(gPausedHandshakeRespondersMutex);
    gHandshakeRespondersPausedForTest.store(paused, std::memory_order_release);
    if (!paused) {
      wakeups.swap(gPausedHandshakeResponderWakeups);
    }
  }
  for (auto& wakeup : wakeups) {
    wakeup();
  }
}

void Acceptor::testingResetHandshakeResponderPeak() {
  gPeakHandshakeResponders.store(
      gActiveHandshakeResponders.load(std::memory_order_acquire),
      std::memory_order_release);
}

size_t Acceptor::testingActiveHandshakeResponders() {
  return gActiveHandshakeResponders.load(std::memory_order_acquire);
}

size_t Acceptor::testingPeakHandshakeResponders() {
  return gPeakHandshakeResponders.load(std::memory_order_acquire);
}

/*static*/
void Acceptor::cStyleAMCallback(
    std::shared_ptr<ucxx::Request> request,
    ucp_ep_h ep) {
  VELOX_CHECK_NOT_NULL(request, "AMCallback called with nullptr request!");
  VELOX_CHECK(
      request->isCompleted(), "AMCallback called with incomplete request!");
  auto buffer =
      std::dynamic_pointer_cast<ucxx::Buffer>(request->getRecvBuffer());
  VELOX_CHECK_NOT_NULL(buffer, "AMCallback: failed to get receive buffer.");
  // A versioned fixed-size request is required. Accepting a prefix would make
  // an older peer's bytes look like a valid destination/worker contract. A
  // malformed active message has no trustworthy response tag, so fail closed
  // without terminating the progress process.
  if (buffer->getSize() != sizeof(HandshakeMsg)) {
    LOG(ERROR) << "Ignoring UCX handshake with size " << buffer->getSize()
               << "; expected " << sizeof(HandshakeMsg);
    return;
  }
  HandshakeMsg handshake;
  std::memcpy(&handshake, buffer->data(), sizeof(handshake));

  // Create a exchangeServer based on the information received in the initial
  // handshake.
  std::shared_ptr<Communicator> communicator = Communicator::getInstance();

  auto epRef = communicator->findEndpointRefByHandle(ep);
  VELOX_CHECK_NOT_NULL(epRef, "Could not find endpoint reference");
  const auto taskIdLength = strnlen(handshake.taskId, sizeof(handshake.taskId));
  const std::string taskId(handshake.taskId, taskIdLength);
  const PartitionKey key{taskId, handshake.destination};

  if (handshake.protocolVersion != kUcxExchangeProtocolVersion ||
      !isCanonicalHandshakeTaskIdBuffer(
          handshake.taskId, sizeof(handshake.taskId)) ||
      !isValidHandshakeTaskId(taskId)) {
    // This callback already runs on the Communicator/UCX progress thread.
    // Immediate rejects are sent here so a reject flood cannot enqueue an
    // unbounded number of responder work items while server reservations are
    // exhausted.
    sendHandshakeResponse(
        epRef, key, HandshakeStatus::kInvalidRequest, false, 0);
    return;
  }

  auto queueMgr = UcxOutputQueueManager::getInstanceRef();
  auto pending =
      PendingHandshake::create(communicator, epRef, key, handshake.workerId);
  const auto reserveAndAccept = [pending,
                                 queueMgr,
                                 taskId = key.taskId,
                                 destination = key.destination,
                                 endpointIdentity = reinterpret_cast<uintptr_t>(
                                     epRef.get())]() {
    auto reservation =
        queueMgr->reserveHandshake(taskId, destination, endpointIdentity);
    if (!reservation) {
      pending->reject(protocolStatus(reservation.rejectReason));
      return;
    }
    pending->accept(reservation.contract, std::move(reservation.reservation));
  };
  const auto admission = queueMgr->admitHandshake(
      key.taskId,
      key.destination,
      reserveAndAccept,
      [pending](UcxTaskLifecycleRegistry::AdmissionRejectReason reason) {
        pending->reject(protocolStatus(reason));
      });

  if (admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kExpected) {
    auto reservation = queueMgr->reserveHandshake(
        key.taskId, key.destination, reinterpret_cast<uintptr_t>(epRef.get()));
    if (!reservation) {
      sendHandshakeResponse(
          epRef,
          key,
          protocolStatus(reservation.rejectReason),
          false,
          reservation.contract.destinationCount);
      return;
    }
    pending->accept(reservation.contract, std::move(reservation.reservation));
  } else if (
      admission.disposition ==
      UcxTaskLifecycleRegistry::RequestDisposition::kRejected) {
    sendHandshakeResponse(
        epRef,
        key,
        protocolStatus(admission.rejectReason),
        false,
        admission.contract.destinationCount);
  }
}

// Add endpoint reference to ucp_cp -> epRef map.
void Acceptor::registerEndpointRef(std::shared_ptr<EndpointRef> endpointRef) {
  auto epHandle = endpointRef->endpoint_->getHandle();
  auto res = handleToEndpointRef_.insert(std::pair{epHandle, endpointRef});
  VELOX_CHECK(res.second, "Endpoint handle already exists!");
}
} // namespace facebook::velox::ucx_exchange
