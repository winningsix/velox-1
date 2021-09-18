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
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
/**
 * Cider JITed operator which fuses all operators when possible. Underlying, it
 * uses OmnisciDB as an execution engine.
 */
class CiderJITedOperator : public Operator {
 public:
  CiderJITedOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::JITedNode>& jitedNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override;

  void finish() override;

 private:
  RowVectorPtr input_;

  // public:
//  CiderJITedOperator(
//      int32_t operatorId,
//      DriverCtx* driverCtx,
//      const std::shared_ptr<const core::JITedNode>& jitedNode);
//
//  bool needsInput() const override;
//
//  void addInput(RowVectorPtr input) override;
//
//  RowVectorPtr getOutput() override;
//
//  BlockingReason isBlocked(ContinueFuture* /*future*/) override;
//
//  void finish() override;

// private:
//  bool contain_join_build_ = false;
//  bool contain_join_probe_ = false;
//  bool contain_blocking_ops_ = false;
//  // Cider RelAlg node string for all blocking string。 It will be called at
//  // addInput method. Intermediate data structure is employed and return when
//  // getOutput method is called.
//  const std::string blocking_cider_str_ = "";
//
//  // Cider RelAlg node string for all non-blocking string
//  // It will be called at getOutput method
//  const std::string non_blocking_cider_str_= "";
//  RowVectorPtr input_;
//  // For join build, it will be broadcast to pair drivers like join op
//  // TODO (Cheng) change to Arrow format or other format
//  RowVectorPtr non_blocking_res_;
};

} // namespace facebook::velox::exec
