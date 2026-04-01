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

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
// #endregion
#include "velox/experimental/cudf/exec/GpuGuard.h"
#include "velox/experimental/cudf/exec/BloomFilterKernels.h"
#include "velox/experimental/cudf/exec/DecimalAggregationKernels.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"

#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/PrefixSort.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/reduction.hpp>
#include <cudf/reduction/approx_distinct_count.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/unary.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/error.hpp>

#include <chrono>
#include <cmath>
#include <stdexcept>
#include <thread>
#include <typeinfo>
#include <vector>

namespace {

using namespace facebook::velox;
using namespace facebook::velox::cudf_velox;

#define DEFINE_SIMPLE_AGGREGATOR(Name, name, KIND)                            \
  struct Name##Aggregator : cudf_velox::CudfHashAggregation::Aggregator {     \
    Name##Aggregator(                                                         \
        core::AggregationNode::Step step,                                     \
        uint32_t inputIndex,                                                  \
        VectorPtr constant,                                                   \
        bool is_global,                                                       \
        const TypePtr& resultType)                                            \
        : Aggregator(                                                         \
              step,                                                           \
              cudf::aggregation::KIND,                                        \
              inputIndex,                                                     \
              constant,                                                       \
              is_global,                                                      \
              resultType) {}                                                  \
                                                                              \
    void addGroupbyRequest(                                                   \
        cudf::table_view const& tbl,                                          \
        std::vector<cudf::groupby::aggregation_request>& requests,            \
        rmm::cuda_stream_view stream) override {                              \
      VELOX_CHECK(                                                            \
          constant == nullptr,                                                \
          #Name "Aggregator does not yet support constant input");            \
      auto& request = requests.emplace_back();                                \
      output_idx = requests.size() - 1;                                       \
      request.values = tbl.column(inputIndex);                                \
      request.aggregations.push_back(                                         \
          cudf::make_##name##_aggregation<cudf::groupby_aggregation>());      \
    }                                                                         \
                                                                              \
    std::unique_ptr<cudf::column> makeOutputColumn(                           \
        std::vector<cudf::groupby::aggregation_result>& results,              \
        rmm::cuda_stream_view stream) override {                              \
      if (output_idx >= results.size() ||                                     \
          results[output_idx].results.empty()) {                              \
        return nullptr;                                                       \
      }                                                                       \
      auto col = std::move(results[output_idx].results[0]);                   \
      auto const cudfResType = cudf_velox::veloxToCudfDataType(resultType);   \
      if (col->type() != cudfResType) {                                       \
        col = cudf::cast(*col, cudfResType, stream, cudf::get_current_device_resource_ref());         \
      }                                                                       \
      return col;                                                             \
    }                                                                         \
                                                                              \
    std::unique_ptr<cudf::column> doReduce(                                   \
        cudf::table_view const& input,                                        \
        TypePtr const& outputType,                                            \
        rmm::cuda_stream_view stream) override {                              \
      auto const aggRequest =                                                 \
          cudf::make_##name##_aggregation<cudf::reduce_aggregation>();        \
      auto const cudfOutType = cudf_velox::veloxToCudfDataType(outputType);   \
      auto const resultScalar = cudf::reduce(                                 \
          input.column(inputIndex), *aggRequest, cudfOutType, stream,         \
          cudf::get_current_device_resource_ref());                                                     \
      return cudf::make_column_from_scalar(*resultScalar, 1, stream,          \
        cudf::get_current_device_resource_ref());                                                     \
    }                                                                         \
                                                                              \
   private:                                                                   \
    uint32_t output_idx;                                                      \
  };

DEFINE_SIMPLE_AGGREGATOR(Sum, sum, SUM)
DEFINE_SIMPLE_AGGREGATOR(Min, min, MIN)
DEFINE_SIMPLE_AGGREGATOR(Max, max, MAX)

struct DecimalSumOrAvgAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  DecimalSumOrAvgAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType,
      const bool isAvg)
      : Aggregator(
            step,
            cudf::aggregation::SUM,
            inputIndex,
            constant,
            isGlobal,
            resultType),
        isAvg_(isAvg) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    if ((step == core::AggregationNode::Step::kFinal ||
         step == core::AggregationNode::Step::kIntermediate) &&
        tbl.column(inputIndex).type().id() == cudf::type_id::STRUCT) {
      auto const& structCol = tbl.column(inputIndex);
      // Deep copy children to standalone columns for proper alignment
      {
        auto rawCopy = std::make_unique<cudf::column>(
            structCol.child(0), stream, cudf::get_current_device_resource_ref());
        origSumType_ = rawCopy->type();
        if (rawCopy->type().id() == cudf::type_id::DECIMAL128) {
          sumCopy_ = cudf::cast(
              *rawCopy,
              cudf::data_type{cudf::type_id::DECIMAL64, rawCopy->type().scale()},
              stream, cudf::get_current_device_resource_ref());
        } else {
          sumCopy_ = std::move(rawCopy);
        }
      }
      countCopy_ = std::make_unique<cudf::column>(
          structCol.child(1), stream, cudf::get_current_device_resource_ref());
      sumIdx_ = requests.size();
      auto& sumRequest = requests.emplace_back();
      sumRequest.values = sumCopy_->view();
      sumRequest.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      if (isAvg_ || step == core::AggregationNode::Step::kIntermediate) {
        countIdx_ = requests.size();
        auto& countRequest = requests.emplace_back();
        countRequest.values = countCopy_->view();
        countRequest.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      }
      return;
    }
    if (step == core::AggregationNode::Step::kIntermediate &&
        tbl.column(inputIndex).type().id() == cudf::type_id::STRING) {
      auto scale = resultType->isDecimal()
          ? getDecimalPrecisionScale(*resultType).second
          : 0;
      auto decoded = cudf_velox::deserializeDecimalSumStateWithCount(
          tbl.column(inputIndex), scale, stream, cudf::get_current_device_resource_ref());
      decodedSum_ = std::move(decoded.sum);
      decodedCount_ = std::move(decoded.count);

      sumIdx_ = requests.size();
      auto& sumRequest = requests.emplace_back();
      sumRequest.values = decodedSum_->view();
      sumRequest.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());

      countIdx_ = requests.size();
      auto& countRequest = requests.emplace_back();
      countRequest.values = decodedCount_->view();
      countRequest.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      return;
    }

    // STRUCT intermediate state arrives when shuffle preserves the
    // ROW<DECIMAL128, INT64> structure (e.g. after join → agg pipeline).
    if (step == core::AggregationNode::Step::kIntermediate &&
        tbl.column(inputIndex).type().id() == cudf::type_id::STRUCT) {
      auto& col = tbl.column(inputIndex);
      sumIdx_ = requests.size();
      auto& sumRequest = requests.emplace_back();
      sumRequest.values = col.child(0);
      sumRequest.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());

      countIdx_ = requests.size();
      auto& countRequest = requests.emplace_back();
      countRequest.values = col.child(1);
      countRequest.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      return;
    }

    if (step == core::AggregationNode::Step::kFinal &&
        tbl.column(inputIndex).type().id() == cudf::type_id::STRING) {
      auto scale = getDecimalPrecisionScale(*resultType).second;
      if (isAvg_) {
        auto decoded = cudf_velox::deserializeDecimalSumStateWithCount(
            tbl.column(inputIndex), scale, stream, cudf::get_current_device_resource_ref());
        decodedSum_ = std::move(decoded.sum);
        decodedCount_ = std::move(decoded.count);

        sumIdx_ = requests.size();
        auto& sumRequest = requests.emplace_back();
        sumRequest.values = decodedSum_->view();
        sumRequest.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());

        countIdx_ = requests.size();
        auto& countRequest = requests.emplace_back();
        countRequest.values = decodedCount_->view();
        countRequest.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        return;
      } else {
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        decodedSum_ = cudf_velox::deserializeDecimalSumState(
            tbl.column(inputIndex), scale, stream, cudf::get_current_device_resource_ref());
        request.values = decodedSum_->view();
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        return;
      }
    }

    if (step == core::AggregationNode::Step::kFinal &&
        tbl.column(inputIndex).type().id() == cudf::type_id::STRUCT) {
      auto& col = tbl.column(inputIndex);
      if (isAvg_) {
        sumIdx_ = requests.size();
        auto& sumRequest = requests.emplace_back();
        sumRequest.values = col.child(0);
        sumRequest.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());

        countIdx_ = requests.size();
        auto& countRequest = requests.emplace_back();
        countRequest.values = col.child(1);
        countRequest.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      } else {
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        request.values = col.child(0);
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      }
      return;
    }

    {
      auto& request = requests.emplace_back();
      sumIdx_ = requests.size() - 1;
      request.values = tbl.column(inputIndex);
      request.aggregations.push_back(
          cudf::make_sum_aggregation<cudf::groupby_aggregation>());
      if (step == core::AggregationNode::Step::kPartial ||
          (step == core::AggregationNode::Step::kSingle && isAvg_)) {
        request.aggregations.push_back(
            cudf::make_count_aggregation<cudf::groupby_aggregation>(
                cudf::null_policy::EXCLUDE));
      }
      return;
    }
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    if (sumIdx_ >= results.size() || results[sumIdx_].results.empty()) {
      return nullptr;
    }
    auto col = std::move(results[sumIdx_].results[0]);
    if (isAvg_ && step == core::AggregationNode::Step::kSingle) {
      if (results[sumIdx_].results.size() < 2) return nullptr;
      auto count = std::move(results[sumIdx_].results[1]);
      return computeAvgColumn(std::move(col), std::move(count), stream);
    }
    if (step == core::AggregationNode::Step::kPartial) {
      if (results[sumIdx_].results.size() < 2) return nullptr;
      auto count = std::move(results[sumIdx_].results[1]);
      if (count->type().id() != cudf::type_id::INT64) {
        count =
            cudf::cast(*count, cudf::data_type{cudf::type_id::INT64}, stream, cudf::get_current_device_resource_ref());
      }
      auto size = col->size();
      std::vector<std::unique_ptr<cudf::column>> children;
      children.push_back(std::move(col));
      children.push_back(std::move(count));
      return std::make_unique<cudf::column>(
          cudf::data_type(cudf::type_id::STRUCT),
          size,
          rmm::device_buffer{},
          rmm::device_buffer{},
          0,
          std::move(children));
    }
    if (step == core::AggregationNode::Step::kIntermediate) {
      if (countIdx_ >= results.size() || results[countIdx_].results.empty())
        return nullptr;
      auto count = std::move(results[countIdx_].results[0]);
      if (count->type().id() != cudf::type_id::INT64) {
        count =
            cudf::cast(*count, cudf::data_type{cudf::type_id::INT64}, stream, cudf::get_current_device_resource_ref());
      }
      if (origSumType_.id() == cudf::type_id::DECIMAL128 &&
          col->type().id() == cudf::type_id::DECIMAL64) {
        col = cudf::cast(*col, origSumType_, stream,
                         cudf::get_current_device_resource_ref());
      }
      auto size = col->size();
      std::vector<std::unique_ptr<cudf::column>> children;
      children.push_back(std::move(col));
      children.push_back(std::move(count));
      return std::make_unique<cudf::column>(
          cudf::data_type(cudf::type_id::STRUCT),
          size,
          rmm::device_buffer{},
          rmm::device_buffer{},
          0,
          std::move(children));
    }
    if (isAvg_ && step == core::AggregationNode::Step::kFinal) {
      if (origSumType_.id() == cudf::type_id::DECIMAL128 &&
          col->type().id() == cudf::type_id::DECIMAL64) {
        col = cudf::cast(*col, origSumType_, stream,
                         cudf::get_current_device_resource_ref());
      }
      if (countIdx_ >= results.size() || results[countIdx_].results.empty())
        return nullptr;
      auto count = std::move(results[countIdx_].results[0]);
      return computeAvgColumn(std::move(col), std::move(count), stream);
    }
    auto const cudfResType = cudf_velox::veloxToCudfDataType(resultType);
    if (col->type() != cudfResType) {
      col = cudf::cast(*col, cudfResType, stream, cudf::get_current_device_resource_ref());
    }
    return col;
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    if (step == core::AggregationNode::Step::kSingle && isAvg_) {
      auto const sumAgg =
          cudf::make_sum_aggregation<cudf::reduce_aggregation>();
      cudf::column_view inputCol = input.column(inputIndex);
      auto sumScalar = cudf::reduce(inputCol, *sumAgg, inputCol.type(), stream, cudf::get_current_device_resource_ref());
      auto countAgg = cudf::make_count_aggregation<cudf::reduce_aggregation>(
          cudf::null_policy::EXCLUDE);
      auto countScalar = cudf::reduce(
          inputCol, *countAgg, cudf::data_type{cudf::type_id::INT64}, stream, cudf::get_current_device_resource_ref());
      auto sumCol = cudf::make_column_from_scalar(*sumScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto countCol = cudf::make_column_from_scalar(*countScalar, 1, stream, cudf::get_current_device_resource_ref());
      return computeAvgColumn(std::move(sumCol), std::move(countCol), stream);
    }
    auto const aggRequest =
        cudf::make_sum_aggregation<cudf::reduce_aggregation>();
    cudf::column_view inputCol = input.column(inputIndex);
    if (step == core::AggregationNode::Step::kPartial) {
      auto sumScalar =
          cudf::reduce(inputCol, *aggRequest, inputCol.type(), stream, cudf::get_current_device_resource_ref());
      auto countAgg = cudf::make_count_aggregation<cudf::reduce_aggregation>(
          cudf::null_policy::EXCLUDE);
      auto countScalar = cudf::reduce(
          inputCol, *countAgg, cudf::data_type{cudf::type_id::INT64}, stream, cudf::get_current_device_resource_ref());
      auto sumCol = cudf::make_column_from_scalar(*sumScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto countCol = cudf::make_column_from_scalar(*countScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto const cudfSumType = cudf_velox::veloxToCudfDataType(
          asRowType(outputType)->childAt(0));
      if (sumCol->type() != cudfSumType) {
        sumCol = cudf::cast(*sumCol, cudfSumType, stream,
                            cudf::get_current_device_resource_ref());
      }
      std::vector<std::unique_ptr<cudf::column>> children;
      children.push_back(std::move(sumCol));
      children.push_back(std::move(countCol));
      return std::make_unique<cudf::column>(
          cudf::data_type(cudf::type_id::STRUCT),
          1,
          rmm::device_buffer{},
          rmm::device_buffer{},
          0,
          std::move(children));
    }
    if (step == core::AggregationNode::Step::kIntermediate &&
        inputCol.type().id() == cudf::type_id::STRING) {
      auto scale = outputType->isDecimal()
          ? getDecimalPrecisionScale(*outputType).second
          : 0;
      auto decoded = cudf_velox::deserializeDecimalSumStateWithCount(
          inputCol, scale, stream, cudf::get_current_device_resource_ref());
      auto sumScalar = cudf::reduce(
          decoded.sum->view(), *aggRequest, decoded.sum->view().type(), stream, cudf::get_current_device_resource_ref());
      auto countScalar = cudf::reduce(
          decoded.count->view(),
          *aggRequest,
          cudf::data_type{cudf::type_id::INT64},
          stream, cudf::get_current_device_resource_ref());
      auto sumCol = cudf::make_column_from_scalar(*sumScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto countCol = cudf::make_column_from_scalar(*countScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto const cudfSumType = cudf_velox::veloxToCudfDataType(
          asRowType(outputType)->childAt(0));
      if (sumCol->type() != cudfSumType) {
        sumCol = cudf::cast(*sumCol, cudfSumType, stream,
                            cudf::get_current_device_resource_ref());
      }
      std::vector<std::unique_ptr<cudf::column>> children;
      children.push_back(std::move(sumCol));
      children.push_back(std::move(countCol));
      return std::make_unique<cudf::column>(
          cudf::data_type(cudf::type_id::STRUCT),
          1,
          rmm::device_buffer{},
          rmm::device_buffer{},
          0,
          std::move(children));
    }
    if ((step == core::AggregationNode::Step::kFinal ||
         step == core::AggregationNode::Step::kIntermediate) &&
        inputCol.type().id() == cudf::type_id::STRUCT) {
      auto const sumChild = inputCol.child(0);
      auto const countChild = inputCol.child(1);
      auto sumScalar = cudf::reduce(
          sumChild, *aggRequest, sumChild.type(), stream,
          cudf::get_current_device_resource_ref());
      auto sumCol = cudf::make_column_from_scalar(
          *sumScalar, 1, stream, cudf::get_current_device_resource_ref());
      if (isAvg_ && step == core::AggregationNode::Step::kFinal) {
        auto countScalar = cudf::reduce(
            countChild, *aggRequest,
            cudf::data_type{cudf::type_id::INT64}, stream,
            cudf::get_current_device_resource_ref());
        auto countCol = cudf::make_column_from_scalar(
            *countScalar, 1, stream, cudf::get_current_device_resource_ref());
        return computeAvgColumn(
            std::move(sumCol), std::move(countCol), stream);
      }
      auto countScalar = cudf::reduce(
          countChild, *aggRequest,
          cudf::data_type{cudf::type_id::INT64}, stream,
          cudf::get_current_device_resource_ref());
      auto countCol = cudf::make_column_from_scalar(
          *countScalar, 1, stream, cudf::get_current_device_resource_ref());
      auto const cudfSumType = cudf_velox::veloxToCudfDataType(
          asRowType(outputType)->childAt(0));
      if (sumCol->type() != cudfSumType) {
        sumCol = cudf::cast(*sumCol, cudfSumType, stream,
                            cudf::get_current_device_resource_ref());
      }
      std::vector<std::unique_ptr<cudf::column>> children;
      children.push_back(std::move(sumCol));
      children.push_back(std::move(countCol));
      return std::make_unique<cudf::column>(
          cudf::data_type(cudf::type_id::STRUCT), 1,
          rmm::device_buffer{}, rmm::device_buffer{}, 0,
          std::move(children));
    }
    if (step == core::AggregationNode::Step::kFinal &&
        inputCol.type().id() == cudf::type_id::STRING) {
      auto scale = getDecimalPrecisionScale(*outputType).second;
      if (isAvg_) {
        // AVG
        // deserialize the results (sum and count)
        auto sumAndCount = cudf_velox::deserializeDecimalSumStateWithCount(
            inputCol, scale, stream, cudf::get_current_device_resource_ref());
        // reduce the two results to get final sum and count scalars
        auto sumScalar = cudf::reduce(
            sumAndCount.sum->view(),
            *aggRequest,
            sumAndCount.sum->view().type(),
            stream, cudf::get_current_device_resource_ref());
        auto countScalar = cudf::reduce(
            sumAndCount.count->view(),
            *aggRequest,
            cudf::data_type{cudf::type_id::INT64},
            stream, cudf::get_current_device_resource_ref());
        // convert to columns in order to perform division, as we cannot divide
        // scalars directly
        auto sumCol = cudf::make_column_from_scalar(*sumScalar, 1, stream, cudf::get_current_device_resource_ref());
        auto countCol = cudf::make_column_from_scalar(*countScalar, 1, stream, cudf::get_current_device_resource_ref());
        return computeAvgColumn(std::move(sumCol), std::move(countCol), stream);
      } else {
        // SUM
        decodedSum_ =
            cudf_velox::deserializeDecimalSumState(inputCol, scale, stream, cudf::get_current_device_resource_ref());
        inputCol = decodedSum_->view();
        // @TODO does this need to drop through to the code below
        // or can we just do that stuff here, and not need decodedSum_ or
        // decodedCount_ why we do have those anyway if they're only set in
        // addGroupbyRequest() and either overwritten or not even used here?
        // what does the final cudf::reduce() below actually do?
      }
    }
    auto const cudfOutType = cudf_velox::veloxToCudfDataType(outputType);
    std::unique_ptr<cudf::column> castedInput;
    if (outputType->isDecimal() && inputCol.type() != cudfOutType) {
      castedInput = cudf::cast(inputCol, cudfOutType, stream, cudf::get_current_device_resource_ref());
      inputCol = castedInput->view();
    }
    auto const resultScalar =
        cudf::reduce(inputCol, *aggRequest, cudfOutType, stream, cudf::get_current_device_resource_ref());
    return cudf::make_column_from_scalar(*resultScalar, 1, stream, cudf::get_current_device_resource_ref());
  }

 private:
  std::unique_ptr<cudf::column> computeAvgColumn(
      std::unique_ptr<cudf::column> sum,
      std::unique_ptr<cudf::column> count,
      rmm::cuda_stream_view stream) const {
    if (count->type().id() != cudf::type_id::INT64) {
      count = cudf::cast(*count, cudf::data_type{cudf::type_id::INT64}, stream, cudf::get_current_device_resource_ref());
    }
    auto avgCol =
        cudf_velox::computeDecimalAverage(sum->view(), count->view(), stream, cudf::get_current_device_resource_ref());
    auto const cudfOutType = cudf_velox::veloxToCudfDataType(resultType);
    if (avgCol->type() != cudfOutType) {
      avgCol = cudf::cast(avgCol->view(), cudfOutType, stream, cudf::get_current_device_resource_ref());
    }
    return avgCol;
  }

  uint32_t sumIdx_{0};
  uint32_t countIdx_{0};
  const bool isAvg_{false};
  std::unique_ptr<cudf::column> decodedSum_;
  std::unique_ptr<cudf::column> decodedCount_;
  std::unique_ptr<cudf::column> alignedSum_;
  std::unique_ptr<cudf::column> sumCopy_;
  cudf::data_type origSumType_{cudf::type_id::DECIMAL128};
  std::unique_ptr<cudf::column> countCopy_;
  std::unique_ptr<cudf::column> alignedCount_;
};

struct CountAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  CountAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType)
      : Aggregator(
            step,
            cudf::aggregation::COUNT_VALID,
            inputIndex,
            constant,
            isGlobal,
            resultType) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    auto& request = requests.emplace_back();
    outputIdx_ = requests.size() - 1;
    request.values = tbl.column(constant == nullptr ? inputIndex : 0);
    std::unique_ptr<cudf::groupby_aggregation> aggRequest =
        exec::isRawInput(step)
        ? cudf::make_count_aggregation<cudf::groupby_aggregation>(
              constant == nullptr ? cudf::null_policy::EXCLUDE
                                  : cudf::null_policy::INCLUDE)
        : cudf::make_sum_aggregation<cudf::groupby_aggregation>();
    request.aggregations.push_back(std::move(aggRequest));
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    if (exec::isRawInput(step)) {
      int64_t count;
      if (constant != nullptr) {
        // count(1) / count(*): just count all rows, no column access needed.
        // Spark's column pruning for df.count() may produce 0-column tables.
        count = input.num_rows();
      } else if (inputIndex < input.num_columns()) {
        auto inputCol = input.column(inputIndex);
        count = inputCol.size() - inputCol.null_count();
      } else {
        count = input.num_rows();
      }

      auto resultScalar = cudf::numeric_scalar<int64_t>(count);
      return cudf::make_column_from_scalar(resultScalar, 1, stream);
    } else {
      // For non-raw input (intermediate/final), use sum aggregation.
      // Guard against 0-column tables from empty-schema shuffles.
      if (inputIndex >= input.num_columns()) {
        int64_t fallback = input.num_rows();
        auto resultScalar = cudf::numeric_scalar<int64_t>(fallback);
        resultScalar.set_valid_async(true, stream);
        return cudf::make_column_from_scalar(resultScalar, 1, stream);
      }
      auto const aggRequest =
          cudf::make_sum_aggregation<cudf::reduce_aggregation>();
      auto const cudfOutputType = cudf::data_type(cudf::type_id::INT64);
      auto const resultScalar = cudf::reduce(
          input.column(inputIndex), *aggRequest, cudfOutputType, stream);
      resultScalar->set_valid_async(true, stream);
      return cudf::make_column_from_scalar(*resultScalar, 1, stream);
    }
    return nullptr;
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    if (outputIdx_ >= results.size() || results[outputIdx_].results.empty()) {
      return nullptr;
    }
    // cudf produces int32 for count(0) but velox expects int64
    auto col = std::move(results[outputIdx_].results[0]);
    const auto cudfOutputType = cudf_velox::veloxToCudfDataType(resultType);
    if (col->type() != cudfOutputType) {
      col = cudf::cast(*col, cudfOutputType, stream);
    }
    return col;
  }

 private:
  uint32_t outputIdx_;
};

struct MeanAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  MeanAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType)
      : Aggregator(
            step,
            cudf::aggregation::MEAN,
            inputIndex,
            constant,
            isGlobal,
            resultType) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto& request = requests.emplace_back();
        request.values = tbl.column(inputIndex);
        if (cudf::is_fixed_point(tbl.column(inputIndex).type())) {
          sumIdx_ = requests.size() - 1;
          request.aggregations.push_back(
              cudf::make_sum_aggregation<cudf::groupby_aggregation>());
          request.aggregations.push_back(
              cudf::make_count_aggregation<cudf::groupby_aggregation>(
                  cudf::null_policy::EXCLUDE));
          decimalSingleDecompose_ = true;
        } else {
          meanIdx_ = requests.size() - 1;
          request.aggregations.push_back(
              cudf::make_mean_aggregation<cudf::groupby_aggregation>());
        }
        break;
      }
      case core::AggregationNode::Step::kPartial: {
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex);
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        request.aggregations.push_back(
            cudf::make_count_aggregation<cudf::groupby_aggregation>(
                cudf::null_policy::EXCLUDE));
        break;
      }
      case core::AggregationNode::Step::kIntermediate:
      case core::AggregationNode::Step::kFinal: {
        auto& request = requests.emplace_back();
        sumIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex).child(0);
        request.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());

        auto& request2 = requests.emplace_back();
        countIdx_ = requests.size() - 1;
        request2.values = tbl.column(inputIndex).child(1);
        request2.aggregations.push_back(
            cudf::make_sum_aggregation<cudf::groupby_aggregation>());
        break;
      }
      default:
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    auto safeAccess = [&](uint32_t idx) -> bool {
      return idx < results.size() && !results[idx].results.empty();
    };
    const auto& outputType = asRowType(resultType);
    switch (step) {
      case core::AggregationNode::Step::kSingle:
        if (decimalSingleDecompose_) {
          if (!safeAccess(sumIdx_) || results[sumIdx_].results.size() < 2)
            return nullptr;
          auto sum = std::move(results[sumIdx_].results[0]);
          auto count = std::move(results[sumIdx_].results[1]);
          auto const outType = cudf_velox::veloxToCudfDataType(resultType);
          return cudf::binary_operation(
              *sum, *count, cudf::binary_operator::DIV,
              outType, stream, cudf::get_current_device_resource_ref());
        }
        if (!safeAccess(meanIdx_)) return nullptr;
        return std::move(results[meanIdx_].results[0]);
      case core::AggregationNode::Step::kPartial: {
        if (!safeAccess(sumIdx_) || results[sumIdx_].results.size() < 2)
          return nullptr;
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[sumIdx_].results[1]);

        auto const size = sum->size();
        auto const cudfSumType =
            cudf_velox::veloxToCudfDataType(outputType->childAt(0));
        auto const cudfCountType =
            cudf_velox::veloxToCudfDataType(outputType->childAt(1));
        if (sum->type() != cudfSumType) {
          sum = cudf::cast(*sum, cudfSumType, stream, cudf::get_current_device_resource_ref());
        }
        if (count->type() != cudf::data_type(cudfCountType)) {
          count = cudf::cast(*count, cudf::data_type(cudfCountType), stream);
        }

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sum));
        children.push_back(std::move(count));

        // TODO: Handle nulls. This can happen if all values are null in a
        // group.
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            size,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kIntermediate: {
        if (!safeAccess(sumIdx_) || !safeAccess(countIdx_)) return nullptr;
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[countIdx_].results[0]);

        auto size = sum->size();
        auto const cudfSumType =
            cudf_velox::veloxToCudfDataType(outputType->childAt(0));
        auto const cudfCountType =
            cudf_velox::veloxToCudfDataType(outputType->childAt(1));
        if (sum->type() != cudfSumType) {
          sum = cudf::cast(*sum, cudfSumType, stream, cudf::get_current_device_resource_ref());
        }
        if (count->type() != cudf::data_type(cudfCountType)) {
          count = cudf::cast(*count, cudf::data_type(cudfCountType), stream);
        }

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sum));
        children.push_back(std::move(count));

        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            size,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kFinal: {
        if (!safeAccess(sumIdx_) || !safeAccess(countIdx_)) return nullptr;
        auto sum = std::move(results[sumIdx_].results[0]);
        auto count = std::move(results[countIdx_].results[0]);
        auto avg = cudf::binary_operation(
            *sum,
            *count,
            cudf::binary_operator::DIV,
            cudf_velox::veloxToCudfDataType(resultType),
            stream,
            cudf::get_current_device_resource_ref());
        return avg;
      }
      default:
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto inputCol = input.column(inputIndex);
        if (cudf::is_fixed_point(inputCol.type())) {
          auto const sumAgg =
              cudf::make_sum_aggregation<cudf::reduce_aggregation>();
          auto sumScalar =
              cudf::reduce(inputCol, *sumAgg, inputCol.type(), stream);
          auto countAgg =
              cudf::make_count_aggregation<cudf::reduce_aggregation>(
                  cudf::null_policy::EXCLUDE);
          auto countScalar = cudf::reduce(
              inputCol, *countAgg,
              cudf::data_type{cudf::type_id::INT64}, stream);
          auto sumCol =
              cudf::make_column_from_scalar(*sumScalar, 1, stream);
          auto countCol =
              cudf::make_column_from_scalar(*countScalar, 1, stream);
          auto const cudfOutputType =
              cudf_velox::veloxToCudfDataType(outputType);
          return cudf::binary_operation(
              *sumCol, *countCol, cudf::binary_operator::DIV,
              cudfOutputType, stream,
              cudf::get_current_device_resource_ref());
        }
        auto const aggRequest =
            cudf::make_mean_aggregation<cudf::reduce_aggregation>();
        auto const cudfOutputType = cudf_velox::veloxToCudfDataType(outputType);
        auto const resultScalar = cudf::reduce(
            input.column(inputIndex), *aggRequest, cudfOutputType, stream);
        return cudf::make_column_from_scalar(*resultScalar, 1, stream);
      }
      case core::AggregationNode::Step::kPartial: {
        VELOX_CHECK(outputType->isRow());
        auto const& rowType = outputType->asRow();
        auto const cudfSumType = cudf_velox::veloxToCudfDataType(rowType.childAt(0));

        auto const aggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const sumResultScalar = cudf::reduce(
            input.column(inputIndex), *aggRequest, cudfSumType, stream);
        auto sumCol =
            cudf::make_column_from_scalar(*sumResultScalar, 1, stream);

        auto countCol = cudf::make_column_from_scalar(
            cudf::numeric_scalar<int64_t>(
                input.column(inputIndex).size() -
                input.column(inputIndex).null_count()),
            1,
            stream);

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sumCol));
        children.push_back(std::move(countCol));
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            1,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kIntermediate: {
        auto const sumCol = input.column(inputIndex).child(0);
        auto const countCol = input.column(inputIndex).child(1);

        VELOX_CHECK(outputType->isRow());
        auto const& rowType = outputType->asRow();
        auto const cudfSumType =
            cudf_velox::veloxToCudfDataType(rowType.childAt(0));
        auto const cudfCountType =
            cudf_velox::veloxToCudfDataType(rowType.childAt(1));

        auto const sumAgg =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const sumScalar =
            cudf::reduce(sumCol, *sumAgg, cudfSumType, stream);
        auto sumResult = cudf::make_column_from_scalar(*sumScalar, 1, stream);

        auto const countAgg =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const countScalar =
            cudf::reduce(countCol, *countAgg, cudfCountType, stream);
        auto countResult =
            cudf::make_column_from_scalar(*countScalar, 1, stream);

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(sumResult));
        children.push_back(std::move(countResult));
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            1,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kFinal: {
        auto const sumCol = input.column(inputIndex).child(0);
        auto const countCol = input.column(inputIndex).child(1);

        auto const sumAggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const sumResultScalar =
            cudf::reduce(sumCol, *sumAggRequest, sumCol.type(), stream);
        auto sumResultCol =
            cudf::make_column_from_scalar(*sumResultScalar, 1, stream);

        auto const countAggRequest =
            cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        auto const countResultScalar =
            cudf::reduce(countCol, *countAggRequest, countCol.type(), stream);

        auto const cudfOutputType = cudf_velox::veloxToCudfDataType(outputType);
        return cudf::binary_operation(
            *sumResultCol,
            *countResultScalar,
            cudf::binary_operator::DIV,
            cudfOutputType,
            stream);
      }
      default:
        VELOX_NYI("Unsupported aggregation step for mean");
    }
  }

 private:
  uint32_t meanIdx_;
  uint32_t sumIdx_;
  uint32_t countIdx_;
  bool decimalSingleDecompose_{false};
};

struct ApproxDistinctAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  static constexpr cudf::null_policy kNullPolicy = cudf::null_policy::EXCLUDE;
  static constexpr cudf::nan_policy kNanPolicy = cudf::nan_policy::NAN_IS_VALID;

  ApproxDistinctAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType,
      std::int32_t precision = 11) // Default 11 matches Velox's 2.3% standard
                                   // error (2^11 = 2048 buckets)
      : Aggregator{step, cudf::aggregation::INVALID, inputIndex, constant, isGlobal, resultType},
        precision_{precision} {
    VELOX_CHECK(
        constant == nullptr,
        "ApproxDistinctAggregator does not support constant input");
    VELOX_CHECK(
        isGlobal,
        "ApproxDistinctAggregator currently only supports global aggregation");
  }

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    VELOX_UNSUPPORTED(
        "approx_distinct is not supported as a group aggregation");
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    VELOX_UNSUPPORTED(
        "approx_distinct is not supported as a group aggregation");
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    if (exec::isRawInput(step)) {
      return doPartialReduce(input, stream);
    } else if (step == core::AggregationNode::Step::kIntermediate) {
      return doIntermediateReduce(input, stream);
    } else {
      return doFinalReduce(input, stream);
    }
  }

 private:
  std::unique_ptr<cudf::column> makeSketchColumn(
      cuda::std::span<cuda::std::byte const> sketch_bytes,
      rmm::cuda_stream_view stream) {
    auto sketch_size = static_cast<cudf::size_type>(sketch_bytes.size());

    cudf::size_type offsets[2] = {0, sketch_size};
    rmm::device_buffer offsets_device{2 * sizeof(cudf::size_type), stream};
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        offsets_device.data(),
        offsets,
        2 * sizeof(cudf::size_type),
        cudaMemcpyHostToDevice,
        stream.value()));

    rmm::device_buffer chars_buffer{sketch_bytes.size(), stream};
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        chars_buffer.data(),
        sketch_bytes.data(),
        sketch_bytes.size(),
        cudaMemcpyDeviceToDevice,
        stream.value()));

    // Sync stream before stack-allocated offsets goes out of scope
    stream.synchronize();

    auto offsets_column = std::make_unique<cudf::column>(
        cudf::data_type{cudf::type_id::INT32},
        2,
        std::move(offsets_device),
        rmm::device_buffer{},
        0);

    return cudf::make_strings_column(
        1,
        std::move(offsets_column),
        std::move(chars_buffer),
        0,
        rmm::device_buffer{});
  }

  template <typename Func>
  auto mergeSketchesAndApply(
      cudf::column_view const& sketch_column,
      Func&& func,
      rmm::cuda_stream_view stream) {
    auto strings_col = cudf::strings_column_view(sketch_column);
    auto offsets_col = strings_col.offsets();
    auto chars_ptr = strings_col.chars_begin(stream);

    auto num_offsets = sketch_column.size() + 1;
    std::vector<cudf::size_type> host_offsets(num_offsets);
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        host_offsets.data(),
        offsets_col.begin<cudf::size_type>(),
        num_offsets * sizeof(cudf::size_type),
        cudaMemcpyDeviceToHost,
        stream.value()));
    stream.synchronize(); // Need host_offsets before proceeding

    cudf::size_type first_offset = host_offsets[0];
    cudf::size_type first_size = host_offsets[1] - first_offset;

    // Copy to mutable aligned buffer - cudf::approx_distinct_count requires
    // non-const span and proper alignment for int32 registers
    rmm::device_buffer aligned_sketch{
        static_cast<std::size_t>(first_size), stream};
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        aligned_sketch.data(),
        chars_ptr + first_offset,
        static_cast<std::size_t>(first_size),
        cudaMemcpyDeviceToDevice,
        stream.value()));

    cudf::approx_distinct_count merged_sketch(
        cuda::std::span<cuda::std::byte>(
            static_cast<cuda::std::byte*>(aligned_sketch.data()), first_size),
        precision_,
        kNullPolicy,
        kNanPolicy,
        stream);

    for (cudf::size_type i = 1; i < sketch_column.size(); ++i) {
      cudf::size_type start_offset = host_offsets[i];
      cudf::size_type end_offset = host_offsets[i + 1];
      cudf::size_type size = end_offset - start_offset;

      if (size > 0) {
        rmm::device_buffer temp_sketch{static_cast<std::size_t>(size), stream};
        CUDF_CUDA_TRY(cudaMemcpyAsync(
            temp_sketch.data(),
            chars_ptr + start_offset,
            size,
            cudaMemcpyDeviceToDevice,
            stream.value()));

        merged_sketch.merge(
            cuda::std::span<cuda::std::byte>(
                static_cast<cuda::std::byte*>(temp_sketch.data()), size),
            stream);
      }
    }

    return func(merged_sketch);
  }

  std::unique_ptr<cudf::column> doPartialReduce(
      cudf::table_view const& input,
      rmm::cuda_stream_view stream) {
    auto inputTable = cudf::table_view({input.column(inputIndex)});

    cudf::approx_distinct_count sketch{
        inputTable, precision_, kNullPolicy, kNanPolicy, stream};

    return makeSketchColumn(sketch.sketch(), stream);
  }

  std::unique_ptr<cudf::column> doIntermediateReduce(
      cudf::table_view const& input,
      rmm::cuda_stream_view stream) {
    auto sketch_column = input.column(inputIndex);

    if (sketch_column.size() == 0) {
      return makeSketchColumn({}, stream);
    }

    return mergeSketchesAndApply(
        sketch_column,
        [this, stream](cudf::approx_distinct_count& sketch) {
          return makeSketchColumn(sketch.sketch(), stream);
        },
        stream);
  }

  std::unique_ptr<cudf::column> doFinalReduce(
      cudf::table_view const& input,
      rmm::cuda_stream_view stream) {
    auto sketch_column = input.column(inputIndex);

    if (sketch_column.size() == 0) {
      return cudf::make_column_from_scalar(
          cudf::numeric_scalar<int64_t>(0, true, stream), 1, stream);
    }

    return mergeSketchesAndApply(
        sketch_column,
        [stream](cudf::approx_distinct_count& sketch) {
          std::size_t estimate = sketch.estimate(stream);
          return cudf::make_column_from_scalar(
              cudf::numeric_scalar<int64_t>(
                  static_cast<int64_t>(estimate), true, stream),
              1,
              stream);
        },
        stream);
  }

  std::int32_t precision_;
};

struct BloomFilterAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  BloomFilterAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType,
      int64_t expectedNumItems,
      int64_t numBits)
      : Aggregator{
            step,
            cudf::aggregation::INVALID,
            inputIndex,
            constant,
            isGlobal,
            resultType},
        expectedNumItems_(expectedNumItems),
        numBits_(numBits) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    VELOX_UNSUPPORTED(
        "bloom_filter_agg is not supported as a grouped aggregation");
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    VELOX_UNSUPPORTED(
        "bloom_filter_agg is not supported as a grouped aggregation");
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    auto mr = rmm::mr::get_current_device_resource_ref();
    if (exec::isRawInput(step)) {
      return doPartialReduce(input, stream, mr);
    } else {
      return doMergeReduce(input, stream, mr);
    }
  }

 private:
  std::unique_ptr<cudf::column> doPartialReduce(
      cudf::table_view const& input,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr) {
    auto const numLongs =
        static_cast<int>(std::max<int64_t>(1, numBits_ / 64));
    auto const numHashes = computeNumHashes(expectedNumItems_, numBits_);

    auto bloomBuf = bloomFilterCreate(numHashes, numLongs, stream, mr);
    auto const inputCol = input.column(inputIndex);
    bloomFilterPut(*bloomBuf, inputCol, stream);

    return bloomFilterToStringsColumn(*bloomBuf, stream, mr);
  }

  std::unique_ptr<cudf::column> doMergeReduce(
      cudf::table_view const& input,
      rmm::cuda_stream_view stream,
      rmm::device_async_resource_ref mr) {
    auto const bloomCol = input.column(inputIndex);
    if (bloomCol.size() == 0) {
      auto emptyScalar =
          cudf::string_scalar(std::string_view{}, false, stream, mr);
      return cudf::make_column_from_scalar(emptyScalar, 1, stream, mr);
    }

    auto mergedBuf = bloomFilterMerge(bloomCol, stream, mr);
    return bloomFilterToStringsColumn(*mergedBuf, stream, mr);
  }

  int64_t expectedNumItems_;
  int64_t numBits_;
};

// Aggregator for stddev_samp, stddev, variance, var_samp.
// Uses cudf's direct STD/VARIANCE for kSingle, and M2-based aggregation
// with Welford merge for partial/intermediate/final steps.
struct VarianceAggregator : cudf_velox::CudfHashAggregation::Aggregator {
  VarianceAggregator(
      core::AggregationNode::Step step,
      uint32_t inputIndex,
      VectorPtr constant,
      bool isGlobal,
      const TypePtr& resultType,
      bool isStdDev,
      int ddof)
      : Aggregator(
            step,
            isStdDev ? cudf::aggregation::STD : cudf::aggregation::VARIANCE,
            inputIndex,
            constant,
            isGlobal,
            resultType),
        isStdDev_(isStdDev),
        ddof_(ddof) {}

  void addGroupbyRequest(
      cudf::table_view const& tbl,
      std::vector<cudf::groupby::aggregation_request>& requests,
      rmm::cuda_stream_view stream) override {
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto& request = requests.emplace_back();
        singleIdx_ = requests.size() - 1;
        if (cudf::is_fixed_point(tbl.column(inputIndex).type())) {
          castedInput_ = cudf::cast(
              tbl.column(inputIndex),
              cudf::data_type{cudf::type_id::FLOAT64},
              stream, cudf::get_current_device_resource_ref());
          request.values = castedInput_->view();
        } else {
          request.values = tbl.column(inputIndex);
        }
        if (isStdDev_) {
          request.aggregations.push_back(
              cudf::make_std_aggregation<cudf::groupby_aggregation>(ddof_));
        } else {
          request.aggregations.push_back(
              cudf::make_variance_aggregation<cudf::groupby_aggregation>(
                  ddof_));
        }
        break;
      }
      case core::AggregationNode::Step::kPartial: {
        auto& request = requests.emplace_back();
        partialIdx_ = requests.size() - 1;
        if (cudf::is_fixed_point(tbl.column(inputIndex).type())) {
          castedInput_ = cudf::cast(
              tbl.column(inputIndex),
              cudf::data_type{cudf::type_id::FLOAT64},
              stream, cudf::get_current_device_resource_ref());
          request.values = castedInput_->view();
        } else {
          request.values = tbl.column(inputIndex);
        }
        request.aggregations.push_back(
            cudf::make_count_aggregation<cudf::groupby_aggregation>(
                cudf::null_policy::EXCLUDE));
        request.aggregations.push_back(
            cudf::make_mean_aggregation<cudf::groupby_aggregation>());
        request.aggregations.push_back(
            cudf::make_m2_aggregation<cudf::groupby_aggregation>());
        break;
      }
      case core::AggregationNode::Step::kIntermediate:
      case core::AggregationNode::Step::kFinal: {
        auto& request = requests.emplace_back();
        mergeIdx_ = requests.size() - 1;
        request.values = tbl.column(inputIndex);
        request.aggregations.push_back(
            cudf::make_merge_m2_aggregation<cudf::groupby_aggregation>());
        break;
      }
      default:
        VELOX_NYI("Unsupported aggregation step for variance/stddev");
    }
  }

  std::unique_ptr<cudf::column> makeOutputColumn(
      std::vector<cudf::groupby::aggregation_result>& results,
      rmm::cuda_stream_view stream) override {
    auto mr = cudf::get_current_device_resource_ref();
    auto safeAccess = [&](uint32_t idx, size_t minResults = 1) -> bool {
      return idx < results.size() && results[idx].results.size() >= minResults;
    };
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        if (!safeAccess(singleIdx_)) return nullptr;
        return std::move(results[singleIdx_].results[0]);
      }
      case core::AggregationNode::Step::kPartial: {
        if (!safeAccess(partialIdx_, 3)) return nullptr;
        auto count = std::move(results[partialIdx_].results[0]);
        auto mean = std::move(results[partialIdx_].results[1]);
        auto m2 = std::move(results[partialIdx_].results[2]);

        auto const& rowType = asRowType(resultType);
        auto const cudfCountType =
            cudf_velox::veloxToCudfDataType(rowType->childAt(0));
        auto const cudfMeanType =
            cudf_velox::veloxToCudfDataType(rowType->childAt(1));
        auto const cudfM2Type =
            cudf_velox::veloxToCudfDataType(rowType->childAt(2));
        if (count->type() != cudfCountType) {
          count = cudf::cast(*count, cudfCountType, stream, mr);
        }
        if (mean->type() != cudfMeanType) {
          mean = cudf::cast(*mean, cudfMeanType, stream, mr);
        }
        if (m2->type() != cudfM2Type) {
          m2 = cudf::cast(*m2, cudfM2Type, stream, mr);
        }

        auto const size = count->size();
        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(count));
        children.push_back(std::move(mean));
        children.push_back(std::move(m2));
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT),
            size,
            rmm::device_buffer{},
            rmm::device_buffer{},
            0,
            std::move(children));
      }
      case core::AggregationNode::Step::kIntermediate: {
        if (!safeAccess(mergeIdx_)) return nullptr;
        auto mergedStruct = std::move(results[mergeIdx_].results[0]);
        auto const& rowType = asRowType(resultType);
        auto const cudfCountType =
            cudf_velox::veloxToCudfDataType(rowType->childAt(0));

        // If MERGE_M2 output count type differs from Velox's expected type,
        // rebuild the struct with the correct types.
        if (mergedStruct->child(0).type() != cudfCountType) {
          auto const size = mergedStruct->size();
          auto mergedCols = mergedStruct->release();
          auto& childCols = mergedCols.children;
          childCols[0] = cudf::cast(
              childCols[0]->view(), cudfCountType, stream, mr);

          auto children = std::vector<std::unique_ptr<cudf::column>>();
          for (auto& c : childCols) {
            children.push_back(std::move(c));
          }
          return std::make_unique<cudf::column>(
              cudf::data_type(cudf::type_id::STRUCT),
              size,
              rmm::device_buffer{},
              rmm::device_buffer{},
              0,
              std::move(children));
        }
        return mergedStruct;
      }
      case core::AggregationNode::Step::kFinal: {
        if (!safeAccess(mergeIdx_)) return nullptr;
        auto mergedStruct = std::move(results[mergeIdx_].results[0]);
        auto const countCol = mergedStruct->view().child(0);
        auto const m2Col = mergedStruct->view().child(2);

        // Cast count to FLOAT64 for division.
        auto countDouble =
            cudf::cast(countCol, cudf::data_type{cudf::type_id::FLOAT64},
                       stream, mr);

        // divisor = count - ddof
        auto ddofScalar =
            cudf::numeric_scalar<double>(static_cast<double>(ddof_));
        auto divisor = cudf::binary_operation(
            *countDouble, ddofScalar, cudf::binary_operator::SUB,
            cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);

        // variance = m2 / divisor
        auto variance = cudf::binary_operation(
            m2Col, *divisor, cudf::binary_operator::DIV,
            cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);

        // Null out rows where count <= ddof (insufficient data for
        // sample statistics). Spark returns NULL in this case.
        auto ddofThreshold =
            cudf::numeric_scalar<int64_t>(static_cast<int64_t>(ddof_));
        auto validMask = cudf::binary_operation(
            countCol, ddofThreshold, cudf::binary_operator::GREATER,
            cudf::data_type{cudf::type_id::BOOL8}, stream, mr);
        auto nullScalar =
            cudf::numeric_scalar<double>(0.0, false, stream);

        if (isStdDev_) {
          auto stddev = cudf::unary_operation(
              *variance, cudf::unary_operator::SQRT, stream, mr);
          return cudf::copy_if_else(
              *stddev, nullScalar, *validMask, stream, mr);
        }
        return cudf::copy_if_else(
            *variance, nullScalar, *validMask, stream, mr);
      }
      default:
        VELOX_NYI("Unsupported aggregation step for variance/stddev");
    }
  }

  std::unique_ptr<cudf::column> doReduce(
      cudf::table_view const& input,
      TypePtr const& outputType,
      rmm::cuda_stream_view stream) override {
    auto mr = cudf::get_current_device_resource_ref();
    switch (step) {
      case core::AggregationNode::Step::kSingle: {
        auto aggReq = isStdDev_
            ? cudf::make_std_aggregation<cudf::reduce_aggregation>(ddof_)
            : cudf::make_variance_aggregation<cudf::reduce_aggregation>(
                  ddof_);
        auto const cudfOutType = cudf::data_type{cudf::type_id::FLOAT64};
        auto inputCol = input.column(inputIndex);
        std::unique_ptr<cudf::column> castedCol;
        if (cudf::is_fixed_point(inputCol.type())) {
          castedCol = cudf::cast(inputCol, cudfOutType, stream, mr);
          inputCol = castedCol->view();
        }
        auto resultScalar = cudf::reduce(
            inputCol, *aggReq, cudfOutType, stream, mr);
        return cudf::make_column_from_scalar(*resultScalar, 1, stream, mr);
      }
      case core::AggregationNode::Step::kPartial: {
        VELOX_CHECK(outputType->isRow());
        auto inputCol = input.column(inputIndex);
        std::unique_ptr<cudf::column> castedCol;
        if (cudf::is_fixed_point(inputCol.type())) {
          castedCol = cudf::cast(
              inputCol, cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);
          inputCol = castedCol->view();
        }
        int64_t n = inputCol.size() - inputCol.null_count();

        auto countCol = cudf::make_column_from_scalar(
            cudf::numeric_scalar<int64_t>(n), 1, stream, mr);

        if (n == 0) {
          auto meanCol = cudf::make_column_from_scalar(
              cudf::numeric_scalar<double>(0.0, false, stream), 1,
              stream, mr);
          auto m2Col = cudf::make_column_from_scalar(
              cudf::numeric_scalar<double>(0.0, false, stream), 1,
              stream, mr);
          auto children = std::vector<std::unique_ptr<cudf::column>>();
          children.push_back(std::move(countCol));
          children.push_back(std::move(meanCol));
          children.push_back(std::move(m2Col));
          return std::make_unique<cudf::column>(
              cudf::data_type(cudf::type_id::STRUCT), 1,
              rmm::device_buffer{}, rmm::device_buffer{}, 0,
              std::move(children));
        }

        auto meanAgg =
            cudf::make_mean_aggregation<cudf::reduce_aggregation>();
        auto meanScalar = cudf::reduce(
            inputCol, *meanAgg,
            cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);
        auto meanCol =
            cudf::make_column_from_scalar(*meanScalar, 1, stream, mr);

        // m2 = variance_pop * n
        auto varPopAgg =
            cudf::make_variance_aggregation<cudf::reduce_aggregation>(0);
        auto varPopScalar = cudf::reduce(
            inputCol, *varPopAgg,
            cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);
        auto varPopCol =
            cudf::make_column_from_scalar(*varPopScalar, 1, stream, mr);
        auto nScalar = cudf::numeric_scalar<double>(static_cast<double>(n));
        auto m2Col = cudf::binary_operation(
            *varPopCol, nScalar, cudf::binary_operator::MUL,
            cudf::data_type{cudf::type_id::FLOAT64}, stream, mr);

        auto children = std::vector<std::unique_ptr<cudf::column>>();
        children.push_back(std::move(countCol));
        children.push_back(std::move(meanCol));
        children.push_back(std::move(m2Col));
        return std::make_unique<cudf::column>(
            cudf::data_type(cudf::type_id::STRUCT), 1,
            rmm::device_buffer{}, rmm::device_buffer{}, 0,
            std::move(children));
      }
      case core::AggregationNode::Step::kIntermediate:
      case core::AggregationNode::Step::kFinal: {
        // Input is struct column with (count, mean, m2) per row.
        // Merge all rows using Welford's parallel algorithm on host.
        auto const structCol = input.column(inputIndex);
        auto const countView = structCol.child(0);
        auto const meanView = structCol.child(1);
        auto const m2View = structCol.child(2);

        auto const numRows = structCol.size();

        // Copy device columns to host memory for merge.
        std::vector<int64_t> countData(numRows);
        std::vector<double> meanData(numRows);
        std::vector<double> m2Data(numRows);
        CUDF_CUDA_TRY(cudaMemcpyAsync(
            countData.data(),
            countView.data<int64_t>(),
            numRows * sizeof(int64_t),
            cudaMemcpyDeviceToHost,
            stream.value()));
        CUDF_CUDA_TRY(cudaMemcpyAsync(
            meanData.data(),
            meanView.data<double>(),
            numRows * sizeof(double),
            cudaMemcpyDeviceToHost,
            stream.value()));
        CUDF_CUDA_TRY(cudaMemcpyAsync(
            m2Data.data(),
            m2View.data<double>(),
            numRows * sizeof(double),
            cudaMemcpyDeviceToHost,
            stream.value()));
        stream.synchronize();

        int64_t mergedN = 0;
        double mergedMean = 0.0;
        double mergedM2 = 0.0;

        for (int i = 0; i < numRows; i++) {
          auto const pn = countData[i];
          if (pn == 0) {
            continue;
          }
          auto const pMean = meanData[i];
          auto const pM2 = m2Data[i];
          auto const newN = mergedN + pn;
          auto const delta = pMean - mergedMean;
          mergedM2 += pM2 + delta * delta *
              static_cast<double>(mergedN) * static_cast<double>(pn) /
              static_cast<double>(newN);
          mergedMean = (mergedMean * static_cast<double>(mergedN) +
                        pMean * static_cast<double>(pn)) /
              static_cast<double>(newN);
          mergedN = newN;
        }

        if (step == core::AggregationNode::Step::kIntermediate) {
          VELOX_CHECK(outputType->isRow());
          auto outCount = cudf::make_column_from_scalar(
              cudf::numeric_scalar<int64_t>(mergedN), 1, stream, mr);
          auto outMean = cudf::make_column_from_scalar(
              cudf::numeric_scalar<double>(mergedMean), 1, stream, mr);
          auto outM2 = cudf::make_column_from_scalar(
              cudf::numeric_scalar<double>(mergedM2), 1, stream, mr);
          auto children = std::vector<std::unique_ptr<cudf::column>>();
          children.push_back(std::move(outCount));
          children.push_back(std::move(outMean));
          children.push_back(std::move(outM2));
          return std::make_unique<cudf::column>(
              cudf::data_type(cudf::type_id::STRUCT), 1,
              rmm::device_buffer{}, rmm::device_buffer{}, 0,
              std::move(children));
        }

        // kFinal: compute the scalar result.
        if (mergedN == 0) {
          auto nullScalar =
              cudf::numeric_scalar<double>(0.0, false, stream);
          return cudf::make_column_from_scalar(nullScalar, 1, stream, mr);
        }
        if (mergedN <= ddof_) {
          // Return null for sample stats when n <= ddof.
          auto nullScalar =
              cudf::numeric_scalar<double>(0.0, false, stream);
          return cudf::make_column_from_scalar(nullScalar, 1, stream, mr);
        }
        double result = mergedM2 / (static_cast<double>(mergedN) - ddof_);
        if (isStdDev_) {
          result = std::sqrt(result);
        }
        auto resultScalar = cudf::numeric_scalar<double>(result, true, stream);
        return cudf::make_column_from_scalar(resultScalar, 1, stream, mr);
      }
      default:
        VELOX_NYI("Unsupported aggregation step for variance/stddev");
    }
  }

 private:
  bool isStdDev_;
  int ddof_;
  uint32_t singleIdx_{0};
  uint32_t partialIdx_{0};
  uint32_t mergeIdx_{0};
  std::unique_ptr<cudf::column> castedInput_;
};

std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator> createAggregator(
    core::AggregationNode::Step step,
    std::string const& kind,
    uint32_t inputIndex,
    VectorPtr constant,
    bool isGlobal,
    const TypePtr& resultType,
    const std::vector<TypePtr>& rawInputTypes = {}) {
  auto prefix = cudf_velox::CudfConfig::getInstance().functionNamePrefix;
  if (kind.rfind(prefix + "sum", 0) == 0) {
    bool isDecimalInput =
        rawInputTypes.size() == 1 && rawInputTypes[0]->isDecimal();
    if (isDecimalInput) {
      return std::make_unique<DecimalSumOrAvgAggregator>(
          step, inputIndex, constant, isGlobal, resultType, false);
    }
    return std::make_unique<SumAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (kind.rfind(prefix + "count", 0) == 0) {
    return std::make_unique<CountAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (kind.rfind(prefix + "min", 0) == 0) {
    return std::make_unique<MinAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (kind.rfind(prefix + "max", 0) == 0) {
    return std::make_unique<MaxAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (kind.rfind(prefix + "avg", 0) == 0) {
    bool isDecimalInput =
        rawInputTypes.size() == 1 && rawInputTypes[0]->isDecimal();
    bool isDecimalRowInput =
        rawInputTypes.size() == 1 && rawInputTypes[0]->isRow() &&
        rawInputTypes[0]->asRow().childAt(0)->isDecimal();
    if (isDecimalInput || isDecimalRowInput || resultType->isDecimal()) {
      return std::make_unique<DecimalSumOrAvgAggregator>(
          step, inputIndex, constant, isGlobal, resultType, true);
    }
    return std::make_unique<MeanAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (kind.rfind(prefix + "approx_distinct", 0) == 0) {
    return std::make_unique<ApproxDistinctAggregator>(
        step, inputIndex, constant, isGlobal, resultType);
  } else if (
      kind.rfind(prefix + "stddev", 0) == 0) {
    // Handles: stddev, stddev_samp (and their companion _partial/_merge etc.)
    return std::make_unique<VarianceAggregator>(
        step, inputIndex, constant, isGlobal, resultType,
        /*isStdDev=*/true, /*ddof=*/1);
  } else if (
      kind.rfind(prefix + "variance", 0) == 0 ||
      kind.rfind(prefix + "var_samp", 0) == 0) {
    // Handles: variance, var_samp (and their companion _partial/_merge etc.)
    return std::make_unique<VarianceAggregator>(
        step, inputIndex, constant, isGlobal, resultType,
        /*isStdDev=*/false, /*ddof=*/1);
  } else if (kind.rfind(prefix + "bloom_filter_agg", 0) == 0) {
    VELOX_UNREACHABLE(
        "bloom_filter_agg uses dedicated path in toAggregators");
  } else {
    VELOX_NYI("Aggregation not yet supported, kind: {}", kind);
  }
}

/// \brief Convert companion function to step for the aggregation function
///
/// Companion functions are functions that are registered in velox along with
/// their main aggregation functions. These are designed to always function
/// with a fixed `step`. This is to allow spark style planNodes where `step` is
/// the property of the aggregation function rather than the planNode.
/// Companion functions allow us to override the planNode's step and use
/// aggregations of different steps in the same planNode
/// If an agg function name contains companionStep keyword, may cause error, now
/// it does not exist.
core::AggregationNode::Step getCompanionStep(
    std::string const& kind,
    core::AggregationNode::Step step) {
  if (kind.ends_with("_merge")) {
    return core::AggregationNode::Step::kIntermediate;
  }

  if (kind.ends_with("_partial")) {
    return core::AggregationNode::Step::kPartial;
  }

  // The format is count_merge_extract_BIGINT or count_merge_extract.
  // In a SINGLE-step aggregation node, _merge_extract companions receive raw
  // input (not intermediate state) so they must use kSingle signatures and
  // aggregation logic rather than kFinal.
  if (kind.find("_merge_extract") != std::string::npos) {
    if (step == core::AggregationNode::Step::kSingle) {
      return core::AggregationNode::Step::kSingle;
    }
    return core::AggregationNode::Step::kFinal;
  }

  return step;
}

std::string getOriginalName(const std::string& kind) {
  if (kind.ends_with("_merge")) {
    return kind.substr(0, kind.size() - std::string("_merge").size());
  }

  if (kind.ends_with("_partial")) {
    return kind.substr(0, kind.size() - std::string("_partial").size());
  }
  // The format is count_merge_extract_BIGINT or count_merge_extract.
  if (auto pos = kind.find("_merge_extract"); pos != std::string::npos) {
    return kind.substr(0, pos);
  }

  return kind;
}

bool hasFinalAggs(
    std::vector<core::AggregationNode::Aggregate> const& aggregates) {
  return std::any_of(aggregates.begin(), aggregates.end(), [](auto const& agg) {
    return agg.call->name().ends_with("_merge_extract");
  });
}

auto toAggregators(
    core::AggregationNode const& aggregationNode,
    exec::OperatorCtx const& operatorCtx) {
  auto const step = aggregationNode.step();
  bool const isGlobal = aggregationNode.groupingKeys().empty();
  auto const& inputRowSchema = aggregationNode.sources()[0]->outputType();
  const auto numKeys = aggregationNode.groupingKeys().size();
  const auto outputType = aggregationNode.outputType();

  std::vector<std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator>>
      aggregators;
  for (auto i = 0; i < aggregationNode.aggregates().size(); ++i) {
    auto const& aggregate = aggregationNode.aggregates()[i];
    std::vector<column_index_t> aggInputs;
    std::vector<VectorPtr> aggConstants;
    for (auto const& arg : aggregate.call->inputs()) {
      if (auto const field =
              dynamic_cast<core::FieldAccessTypedExpr const*>(arg.get())) {
        aggInputs.push_back(inputRowSchema->getChildIdx(field->name()));
      } else if (
          auto constant =
              dynamic_cast<const core::ConstantTypedExpr*>(arg.get())) {
        aggInputs.push_back(kConstantChannel);
        aggConstants.push_back(constant->toConstantVector(operatorCtx.pool()));
      } else {
        VELOX_NYI("Constants and lambdas not yet supported");
      }
    }
    if (aggregate.distinct) {
      VELOX_NYI("De-dup before aggregation is not yet supported");
    }

    auto const kind = aggregate.call->name();
    auto const companionStep = getCompanionStep(kind, step);
    const auto originalName = getOriginalName(kind);
    auto prefix = cudf_velox::CudfConfig::getInstance().functionNamePrefix;

    // bloom_filter_agg has 1-3 args: value [, estimatedNumItems [, numBits]].
    // Handle separately to extract constant parameters.
    if (originalName == prefix + "bloom_filter_agg") {
      uint32_t bloomInputIndex = 0;
      for (size_t j = 0; j < aggInputs.size(); j++) {
        if (aggInputs[j] != kConstantChannel) {
          bloomInputIndex = aggInputs[j];
          break;
        }
      }

      int64_t expectedNumItems = 1000000; // Spark default
      int64_t numBits = 8388608; // Spark default (expectedNumItems * 8)
      if (aggConstants.size() >= 1) {
        auto* flatVec =
            aggConstants[0]->as<ConstantVector<int64_t>>();
        if (flatVec) {
          expectedNumItems = flatVec->valueAt(0);
        }
      }
      if (aggConstants.size() >= 2) {
        auto* flatVec =
            aggConstants[1]->as<ConstantVector<int64_t>>();
        if (flatVec) {
          numBits = flatVec->valueAt(0);
        }
      } else {
        numBits = expectedNumItems * 8;
      }

      TypePtr resultType;
      if (exec::isPartialOutput(companionStep)) {
        resultType =
            exec::resolveIntermediateType(originalName, aggregate.rawInputTypes);
      } else {
        resultType = outputType->childAt(numKeys + i);
      }

      // For _merge_extract companions in a SINGLE node, the actual input
      // may be intermediate VARBINARY rather than raw BIGINT.  Detect this
      // and switch to kFinal so doMergeReduce is invoked.
      auto bloomStep = companionStep;
      if (exec::isRawInput(bloomStep) &&
          kind.find("_merge_extract") != std::string::npos) {
        auto inputType = inputRowSchema->childAt(bloomInputIndex);
        if (inputType->isVarbinary() || inputType->isVarchar()) {
          bloomStep = core::AggregationNode::Step::kFinal;
        }
      }

      aggregators.push_back(std::make_unique<BloomFilterAggregator>(
          bloomStep,
          bloomInputIndex,
          nullptr,
          isGlobal,
          resultType,
          expectedNumItems,
          numBits));
      continue;
    }

    VELOX_CHECK(
        aggInputs.size() <= 1,
        "Multi-input aggregations not yet supported for {}",
        kind);
    if (aggInputs.empty()) {
      aggInputs.push_back(0);
    }

    auto const inputIndex = aggInputs[0];
    auto const constant = aggConstants.empty() ? nullptr : aggConstants[0];
    TypePtr resultType;
    if (exec::isPartialOutput(companionStep)) {
      // For merge steps the raw input is already the intermediate ROW type,
      // which won't match any original-function signature.  Use it directly.
      if (!exec::isRawInput(companionStep) &&
          aggregate.rawInputTypes.size() == 1 &&
          aggregate.rawInputTypes[0]->isRow()) {
        resultType = aggregate.rawInputTypes[0];
      } else {
        resultType = exec::resolveIntermediateType(
            originalName, aggregate.rawInputTypes);
      }
    } else {
      resultType = outputType->childAt(numKeys + i);
    }

    aggregators.push_back(createAggregator(
        companionStep,
        kind,
        inputIndex,
        constant,
        isGlobal,
        resultType,
        aggregate.rawInputTypes));
  }
  return aggregators;
}

auto toIntermediateAggregators(
    core::AggregationNode const& aggregationNode,
    exec::OperatorCtx const& operatorCtx) {
  auto const step = core::AggregationNode::Step::kIntermediate;
  bool const isGlobal = aggregationNode.groupingKeys().empty();
  auto const& inputRowSchema = aggregationNode.outputType();

  std::vector<std::unique_ptr<cudf_velox::CudfHashAggregation::Aggregator>>
      aggregators;
  for (size_t i = 0; i < aggregationNode.aggregates().size(); i++) {
    // Intermediate aggregation has a 1:1 mapping between input and output.
    // We don't need to figure out input from the aggregate function.
    auto const& aggregate = aggregationNode.aggregates()[i];
    auto const inputIndex = aggregationNode.groupingKeys().size() + i;
    auto const kind = aggregate.call->name();
    auto const constant = nullptr;
    const auto originalName = getOriginalName(kind);
    auto const companionStep = getCompanionStep(kind, step);
    auto prefix = cudf_velox::CudfConfig::getInstance().functionNamePrefix;
    if (exec::isPartialOutput(companionStep)) {
      TypePtr resultType;
      if (!exec::isRawInput(companionStep) &&
          aggregate.rawInputTypes.size() == 1 &&
          aggregate.rawInputTypes[0]->isRow()) {
        resultType = aggregate.rawInputTypes[0];
      } else {
        resultType = exec::resolveIntermediateType(
            originalName, aggregate.rawInputTypes);
      }
      if (originalName == prefix + "bloom_filter_agg") {
        aggregators.push_back(std::make_unique<BloomFilterAggregator>(
            companionStep,
            inputIndex,
            nullptr,
            isGlobal,
            resultType,
            1000000,
            8388608));
      } else {
        aggregators.push_back(createAggregator(
            step,
            kind,
            inputIndex,
            constant,
            isGlobal,
            resultType,
            aggregate.rawInputTypes));
      }
    } else {
      // Final step aggregator will not use the intermediate aggregator.
      aggregators.push_back(nullptr);
    }
  }
  return aggregators;
}

} // namespace

namespace facebook::velox::cudf_velox {

CudfHashAggregation::CudfHashAggregation(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<core::AggregationNode const> const& aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "CudfPartialAggregation"
              : "CudfAggregation",
          std::nullopt),
      NvtxHelper(
          nvtx3::rgb{34, 139, 34}, // Forest Green
          operatorId,
          fmt::format("[{}]", aggregationNode->id())),
      aggregationNode_(aggregationNode),
      isPartialOutput_(
          exec::isPartialOutput(aggregationNode->step()) &&
          !hasFinalAggs(aggregationNode->aggregates())),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      isDistinct_(!isGlobal_ && aggregationNode->aggregates().empty()),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()) {}

void CudfHashAggregation::initialize() {
  Operator::initialize();

  inputType_ = aggregationNode_->sources()[0]->outputType();
  ignoreNullKeys_ = aggregationNode_->ignoreNullKeys();
  setupGroupingKeyChannelProjections(
      groupingKeyInputChannels_, groupingKeyOutputChannels_);

  auto const numGroupingKeys = groupingKeyOutputChannels_.size();

  // Velox CPU does optimizations related to pre-grouped keys. This can be
  // done in cudf by passing sort information to cudf::groupby() constructor.
  // We're postponing this for now.

  numAggregates_ = aggregationNode_->aggregates().size();
  aggregators_ = toAggregators(*aggregationNode_, *operatorCtx_);
  intermediateAggregators_ =
      toIntermediateAggregators(*aggregationNode_, *operatorCtx_);

  // Check that aggregate result type match the output type.
  // TODO: This is output schema validation. In velox CPU, it's done using
  // output types reported by aggregation functions. We can't do that in cudf
  // groupby.

  // TODO: Set identity projections used by HashProbe to pushdown dynamic
  // filters to table scan.

  // TODO: Add support for grouping sets and group ids.

  aggregationNode_.reset();
}

void CudfHashAggregation::setupGroupingKeyChannelProjections(
    std::vector<column_index_t>& groupingKeyInputChannels,
    std::vector<column_index_t>& groupingKeyOutputChannels) const {
  VELOX_CHECK(groupingKeyInputChannels.empty());
  VELOX_CHECK(groupingKeyOutputChannels.empty());

  auto const& inputType = aggregationNode_->sources()[0]->outputType();
  auto const& groupingKeys = aggregationNode_->groupingKeys();
  // The map from the grouping key output channel to the input channel.
  //
  // NOTE: grouping key output order is specified as 'groupingKeys' in
  // 'aggregationNode_'.
  std::vector<exec::IdentityProjection> groupingKeyProjections;
  groupingKeyProjections.reserve(groupingKeys.size());
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyProjections.emplace_back(
        exec::exprToChannel(groupingKeys[i].get(), inputType), i);
  }

  groupingKeyInputChannels.reserve(groupingKeys.size());
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyInputChannels.push_back(groupingKeyProjections[i].inputChannel);
  }

  groupingKeyOutputChannels.resize(groupingKeys.size());

  std::iota(
      groupingKeyOutputChannels.begin(), groupingKeyOutputChannels.end(), 0);
}

void CudfHashAggregation::computeIntermediateGroupbyPartial(CudfVectorPtr tbl) {
  auto inputTableStream = tbl->stream();
  auto groupbyOnInput = doGroupByAggregation(
      tbl->getTableView(),
      groupingKeyInputChannels_,
      aggregators_,
      inputTableStream);

  if (!groupbyOnInput) {
    return;
  }

  if (partialOutput_) {
    std::vector<cudf::table_view> tablesToConcat;
    tablesToConcat.push_back(partialOutput_->getTableView());
    tablesToConcat.push_back(groupbyOnInput->getTableView());

    auto partialOutputStream = partialOutput_->stream();
    cudf::detail::join_streams(
        std::vector<rmm::cuda_stream_view>{inputTableStream},
        partialOutputStream);

    std::vector<AlignedTable> concatAlignStorage;
    for (size_t i = 0; i < tablesToConcat.size(); ++i) {
      if (hasDecimal128Misalignment(tablesToConcat[i])) {
        concatAlignStorage.push_back(
            ensureDecimal128Alignment(tablesToConcat[i], partialOutputStream));
        tablesToConcat[i] = concatAlignStorage.back().view;
      }
    }

    std::unique_ptr<cudf::table> concatenatedTable;
    try {
      concatenatedTable =
          cudf::concatenate(tablesToConcat, partialOutputStream);
    } catch (const std::exception& e) {
      LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                   << "]: partial groupby concat failed (" << e.what()
                   << "), syncing and retrying";
      cudaDeviceSynchronize();
      cudaGetLastError();
      concatenatedTable =
          cudf::concatenate(tablesToConcat, partialOutputStream);
    }

    auto compactedOutput = doGroupByAggregation(
        concatenatedTable->view(),
        groupingKeyOutputChannels_,
        intermediateAggregators_,
        partialOutputStream);
    partialOutput_ = compactedOutput;

    if (inputTableStream != partialOutputStream) {
      cudf::detail::join_streams(
          std::vector<rmm::cuda_stream_view>{partialOutputStream}, inputTableStream);
    }
  } else {
    partialOutput_ = groupbyOnInput;
  }
}

void CudfHashAggregation::computeIntermediateDistinctPartial(
    CudfVectorPtr tbl) {
  // For every input, we'll concat with existing distinct results and then do a
  // distinct on the concatenated results.

  auto inputTableStream = tbl->stream();

  if (partialOutput_) {
    // Concatenate the input table with the existing distinct results.
    std::vector<cudf::table_view> tablesToConcat;
    tablesToConcat.push_back(partialOutput_->getTableView());
    tablesToConcat.push_back(tbl->getTableView().select(
        groupingKeyInputChannels_.begin(), groupingKeyInputChannels_.end()));

    auto partialOutputStream = partialOutput_->stream();
    // We need to join the input table stream on the partial output stream to
    // make sure the input table is available when we do the concat.
    cudf::detail::join_streams(
        std::vector<rmm::cuda_stream_view>{inputTableStream},
        partialOutputStream);

    std::vector<AlignedTable> distinctAlignStorage;
    for (size_t i = 0; i < tablesToConcat.size(); ++i) {
      if (hasDecimal128Misalignment(tablesToConcat[i])) {
        distinctAlignStorage.push_back(
            ensureDecimal128Alignment(tablesToConcat[i], partialOutputStream));
        tablesToConcat[i] = distinctAlignStorage.back().view;
      }
    }

    std::unique_ptr<cudf::table> concatenatedTable;
    try {
      concatenatedTable =
          cudf::concatenate(tablesToConcat, partialOutputStream);
    } catch (const std::exception& e) {
      LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                   << "]: partial distinct concat failed (" << e.what()
                   << "), syncing and retrying";
      cudaDeviceSynchronize();
      cudaGetLastError();
      concatenatedTable =
          cudf::concatenate(tablesToConcat, partialOutputStream);
    }

    // Do a distinct on the concatenated results.
    // Keep concatenatedTable alive while we use its view.
    // Must use partialOutputStream since concatenatedTable was produced on it.
    auto distinctOutput = getDistinctKeys(
        concatenatedTable->view(),
        groupingKeyOutputChannels_,
        partialOutputStream);
    partialOutput_ = distinctOutput;

    // The concatenation (and distinct) on partialOutputStream asynchronously
    // reads from tbl's device buffers, which were allocated on
    // inputTableStream.  When tbl goes out of scope the buffers are freed
    // via cudaFreeAsync on inputTableStream.  Without this reverse join the
    // free can race ahead of the read on partialOutputStream.
    if (inputTableStream != partialOutputStream) {
      cudf::detail::join_streams(
          std::vector<rmm::cuda_stream_view>{partialOutputStream}, inputTableStream);
    }
  } else {
    // First time processing, just store the result of the input batch's
    // distinct. Use getTableView() to avoid expensive materialization for
    // packed_table. tbl stays alive during this function call.
    partialOutput_ = getDistinctKeys(
        tbl->getTableView(), groupingKeyInputChannels_, inputTableStream);
  }
}

void CudfHashAggregation::processAccumulatedPartialInputs() {
  if (accumulatedPartialInputs_.empty()) {
    return;
  }

  auto stream = cudfGlobalStreamPool().get_stream();
  auto tbl =
      getConcatenatedTable(accumulatedPartialInputs_, inputType_, stream);

  auto numRows = static_cast<int64_t>(tbl->num_rows());
  auto cudfBatch = std::make_shared<CudfVector>(
      pool(), inputType_, numRows, std::move(tbl), stream);

  accumulatedPartialInputs_.clear();
  accumulatedPartialRows_ = 0;
  accumulatedPartialBytes_ = 0;

  if (isDistinct_) {
    computeIntermediateDistinctPartial(cudfBatch);
  } else {
    computeIntermediateGroupbyPartial(cudfBatch);
  }

  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        "numCoalescedBatches", RuntimeCounter(1));
  }
}

void CudfHashAggregation::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (input->size() == 0) {
    return;
  }
  numInputRows_ += input->size();

  auto cudfInput = std::dynamic_pointer_cast<cudf_velox::CudfVector>(input);
  VELOX_CHECK_NOT_NULL(cudfInput);

  // #region agent log
  try {
  // #endregion

  if (isPartialOutput_ && !isGlobal_) {
    const auto targetBytes = CudfConfig::getInstance().gpuTargetBatchBytes;
    const auto targetRows = CudfConfig::getInstance().gpuTargetBatchRows;
    if (targetBytes > 0 || targetRows > 0) {
      accumulatedPartialRows_ += input->size();
      accumulatedPartialBytes_ += cudfInput->estimateFlatSize();
      accumulatedPartialInputs_.push_back(std::move(cudfInput));
      bool thresholdReached;
      if (targetBytes > 0 && targetRows > 0) {
        thresholdReached =
            accumulatedPartialBytes_ >= targetBytes ||
            accumulatedPartialRows_ >= targetRows;
      } else if (targetBytes > 0) {
        thresholdReached = accumulatedPartialBytes_ >= targetBytes;
      } else {
        thresholdReached = accumulatedPartialRows_ >= targetRows;
      }
      if (thresholdReached) {
        GpuGuard gpuGuard;
        processAccumulatedPartialInputs();
      }
      return;
    }
    {
      GpuGuard gpuGuard;
      if (isDistinct_) {
        computeIntermediateDistinctPartial(cudfInput);
      } else {
        computeIntermediateGroupbyPartial(cudfInput);
      }
    }
    return;
  }

  // Handle final aggregation or global cases.
  // For non-global group-by / distinct final aggregation, do incremental
  // pre-aggregation when accumulated data exceeds the batch threshold.
  // This prevents inputs_ from growing unboundedly (which causes OOM or
  // extreme slowness in getOutput when it tries to concatenate everything).
  if (!isGlobal_ && !isDistinct_) {
    const auto targetBytes = CudfConfig::getInstance().gpuTargetBatchBytes;
    if (targetBytes > 0) {
      inputs_.push_back(std::move(cudfInput));
      int64_t accBytes = 0;
      for (const auto& inp : inputs_) {
        accBytes += inp->estimateFlatSize();
      }
      if (accBytes >= targetBytes && inputs_.size() > 1) {
        bool canPreAggregate = true;
        for (const auto& agg : intermediateAggregators_) {
          if (!agg) { canPreAggregate = false; break; }
        }
        if (canPreAggregate) {
          GpuGuard gpuGuard;
          try {
            auto stream = cudfGlobalStreamPool().get_stream();
            auto batches =
                getConcatenatedTableBatched(inputs_, inputType_, stream);
            inputs_.clear();

            CudfVectorPtr merged;
            for (auto& batch : batches) {
              if (!batch || batch->num_rows() == 0) continue;
              auto batchResult = doGroupByAggregation(
                  batch->view(), groupingKeyInputChannels_,
                  aggregators_, stream);
              batch.reset();
              if (!batchResult) continue;
              if (!merged) {
                merged = std::move(batchResult);
              } else {
                std::vector<cudf::table_view> toConcat;
                toConcat.push_back(merged->getTableView());
                toConcat.push_back(batchResult->getTableView());
                auto concatTbl = cudf::concatenate(
                    toConcat, stream,
                    cudf::get_current_device_resource_ref());
                batchResult.reset();
                merged = doGroupByAggregation(
                    concatTbl->view(), groupingKeyOutputChannels_,
                    intermediateAggregators_, stream);
              }
            }
            if (merged) {
              inputs_.push_back(std::move(merged));
            }
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: incremental pre-aggregation failed: "
                         << e.what() << ", keeping raw inputs";
            cudaDeviceSynchronize();
            cudaGetLastError();
          }
        }
      }
      return;
    }
  }
  inputs_.push_back(std::move(cudfInput));

  } catch (const std::out_of_range& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::addInput out_of_range: {}",
        planNodeId(),
        e.what());
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::addInput exception ({}): {}",
        planNodeId(),
        typeid(e).name(),
        e.what());
  }
}

CudfVectorPtr CudfHashAggregation::doGroupByAggregation(
    cudf::table_view tableView,
    std::vector<column_index_t> const& groupByKeys,
    std::vector<std::unique_ptr<Aggregator>>& aggregators,
    rmm::cuda_stream_view stream) {
  VLOG(1) << "[DIAG] doGroupByAgg:entry node=" << planNodeId()
               << " rows=" << tableView.num_rows()
               << " cols=" << tableView.num_columns()
               << " nKeys=" << groupByKeys.size()
               << " nAgg=" << aggregators.size();

  if (tableView.num_rows() == 0) {
    return nullptr;
  }

  auto aligned = ensureDecimal128Alignment(tableView, stream);
  tableView = aligned.view;

  for (size_t k = 0; k < groupByKeys.size(); ++k) {
    VELOX_CHECK_LT(
        groupByKeys[k],
        tableView.num_columns(),
        "CudfHashAggregation[{}]::doGroupByAgg groupByKey[{}]={} >= num_columns={}",
        planNodeId(),
        k,
        groupByKeys[k],
        tableView.num_columns());
  }

  auto groupbyKeyView =
      tableView.select(groupByKeys.begin(), groupByKeys.end());

  size_t const numGroupingKeys = groupbyKeyView.num_columns();

  cudf::groupby::groupby groupByOwner(
      groupbyKeyView,
      ignoreNullKeys_ ? cudf::null_policy::EXCLUDE
                      : cudf::null_policy::INCLUDE);

  std::vector<cudf::groupby::aggregation_request> requests;
  try {
    for (size_t ai = 0; ai < aggregators.size(); ++ai) {
      auto& aggregator = aggregators[ai];
      if (aggregator->constant == nullptr) {
        VELOX_CHECK_LT(
            aggregator->inputIndex,
            static_cast<uint32_t>(tableView.num_columns()),
            "CudfHashAggregation[{}]::doGroupByAgg aggregator[{}] inputIndex={} >= num_columns={} kind={}",
            planNodeId(),
            ai,
            aggregator->inputIndex,
            tableView.num_columns(),
            static_cast<int>(aggregator->kind));
      }
      aggregator->addGroupbyRequest(tableView, requests, stream);
    }
  } catch (const std::out_of_range& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg addRequest out_of_range: {}",
        planNodeId(),
        e.what());
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg addRequest exception ({}): {}",
        planNodeId(),
        typeid(e).name(),
        e.what());
  }

  // Synchronize stream before aggregate to surface any misaligned-address
  // error from prior async work rather than letting it propagate into the
  // groupby kernels which obscures the real source.
  stream.synchronize();

  std::pair<std::unique_ptr<cudf::table>,
            std::vector<cudf::groupby::aggregation_result>>
      aggregateResult;
  try {
    aggregateResult = groupByOwner.aggregate(requests, stream);
  } catch (const std::out_of_range& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg aggregate out_of_range: {}",
        planNodeId(),
        e.what());
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg aggregate exception ({}): {}",
        planNodeId(),
        typeid(e).name(),
        e.what());
  }
  auto& [groupKeys, results] = aggregateResult;

  for (size_t i = 0; i < results.size(); ++i) {
    VELOX_CHECK(
        !results[i].results.empty(),
        "CudfHashAggregation[{}]::doGroupByAgg results[{}].results is empty (totalResults={})",
        planNodeId(),
        i,
        results.size());
  }

  std::vector<std::unique_ptr<cudf::column>> resultColumns;
  try {
    auto groupKeysColumns = groupKeys->release();
    resultColumns.insert(
        resultColumns.begin(),
        std::make_move_iterator(groupKeysColumns.begin()),
        std::make_move_iterator(groupKeysColumns.end()));

    for (size_t ai = 0; ai < aggregators.size(); ++ai) {
      auto col = aggregators[ai]->makeOutputColumn(results, stream);
      VELOX_CHECK_NOT_NULL(
          col,
          "CudfHashAggregation[{}]::doGroupByAgg aggregator[{}] makeOutputColumn returned null",
          planNodeId(),
          ai);
      resultColumns.push_back(std::move(col));
    }
  } catch (const std::out_of_range& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg makeOutput out_of_range (built={}): {}",
        planNodeId(),
        resultColumns.size(),
        e.what());
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]::doGroupByAgg makeOutput exception ({}, built={}): {}",
        planNodeId(),
        typeid(e).name(),
        resultColumns.size(),
        e.what());
  }

  auto resultTable = std::make_unique<cudf::table>(std::move(resultColumns));

  auto numRows = resultTable->num_rows();

  if (numRows == 0) {
    return nullptr;
  }

  VLOG(1) << "[DIAG] doGroupByAgg:exit node=" << planNodeId()
               << " numRows=" << numRows;
  return std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(resultTable), stream);
}

CudfVectorPtr CudfHashAggregation::doGlobalAggregation(
    cudf::table_view tableView,
    rmm::cuda_stream_view stream) {
  auto globalAligned = ensureDecimal128Alignment(tableView, stream);
  tableView = globalAligned.view;

  auto mr = cudf::get_current_device_resource_ref();
  std::vector<std::unique_ptr<cudf::column>> resultColumns;
  resultColumns.reserve(aggregators_.size());
  for (size_t i = 0; i < aggregators_.size(); i++) {
    resultColumns.push_back(
        aggregators_[i]->doReduce(tableView, aggregators_[i]->resultType, stream));
  }

  return std::make_shared<cudf_velox::CudfVector>(
      pool(),
      outputType_,
      1,
      std::make_unique<cudf::table>(std::move(resultColumns)),
      stream);
}

CudfVectorPtr CudfHashAggregation::getDistinctKeys(
    cudf::table_view tableView,
    std::vector<column_index_t> const& groupByKeys,
    rmm::cuda_stream_view stream) {
  auto selectedView =
      tableView.select(groupByKeys.begin(), groupByKeys.end());

  if (CudfConfig::getInstance().debugEnabled) {
    stream.synchronize();
    for (int c = 0; c < selectedView.num_columns(); ++c) {
      auto col = selectedView.column(c);
      if (col.has_nulls()) {
        LOG(WARNING) << "getDistinctKeys[" << planNodeId()
                     << "] input col " << c
                     << " null_count=" << col.null_count()
                     << "/" << col.size();
      }
    }
  }

  auto result = cudf::distinct(
      selectedView,
      {groupingKeyOutputChannels_.begin(), groupingKeyOutputChannels_.end()},
      cudf::duplicate_keep_option::KEEP_FIRST,
      cudf::null_equality::EQUAL,
      cudf::nan_equality::ALL_EQUAL,
      stream);

  auto numRows = result->num_rows();

  // velox expects nullptr instead of a table with 0 rows
  if (numRows == 0) {
    return nullptr;
  }

  return std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(result), stream);
}

CudfVectorPtr CudfHashAggregation::releaseAndResetPartialOutput() {
  VELOX_DCHECK(!isGlobal_);
  if (!partialOutput_) {
    numInputRows_ = 0;
    return nullptr;
  }
  auto numOutputRows = partialOutput_->size();
  const double aggregationPct =
      numOutputRows == 0 ? 0 : (numOutputRows * 1.0) / numInputRows_ * 100;
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat("flushRowCount", RuntimeCounter(numOutputRows));
    lockedStats->addRuntimeStat("flushTimes", RuntimeCounter(1));
    lockedStats->addRuntimeStat(
        "partialAggregationPct", RuntimeCounter(aggregationPct));
  }

  numInputRows_ = 0;
  // We're moving partialOutput_ to the caller because we want it to be null
  // after this call.
  return std::move(partialOutput_);
}

RowVectorPtr CudfHashAggregation::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  GpuGuard gpuGuard;

  // Handle partial groupby and distinct.
  if (isPartialOutput_ && !isGlobal_) {
    if (partialOutput_ &&
        partialOutput_->estimateFlatSize() >
            maxPartialAggregationMemoryUsage_) {
      return releaseAndResetPartialOutput();
    }
    if (not noMoreInput_) {
      return nullptr;
    }
    if (!partialOutput_ && finished_) {
      return nullptr;
    }
    return releaseAndResetPartialOutput();
  }

  if (finished_) {
    return nullptr;
  }

  if (!isPartialOutput_ && !noMoreInput_) {
    return nullptr;
  }

  if (inputs_.empty()) {
    if (!noMoreInput_) {
      return nullptr;
    }
    finished_ = true;
    if (isGlobal_) {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                   << " emptyInputs globalAgg with makeEmptyTable";
      auto stream = cudfGlobalStreamPool().get_stream();
      auto tbl = getConcatenatedTable(inputs_, inputType_, stream);
      if (tbl->num_columns() == 0) {
        auto mr = cudf::get_current_device_resource_ref();
        std::vector<std::unique_ptr<cudf::column>> cols;
        cols.push_back(cudf::make_numeric_column(
            cudf::data_type(cudf::type_id::INT8),
            0,
            cudf::mask_state::UNALLOCATED,
            stream,
            mr));
        tbl = std::make_unique<cudf::table>(std::move(cols));
      }
      return doGlobalAggregation(tbl->view(), stream);
    }
    return nullptr;
  }

  auto stream = cudfGlobalStreamPool().get_stream();

  size_t totalInputRows = 0;
  for (const auto& inp : inputs_) {
    totalInputRows += inp->size();
  }
  VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
               << " inputsBatches=" << inputs_.size()
               << " totalInputRows=" << totalInputRows
               << " noMoreInput=" << noMoreInput_
               << " global=" << isGlobal_
               << " distinct=" << isDistinct_;

  // Use batched concatenation to prevent cudaErrorInvalidValue (when the
  // combined buffer size exceeds cuDF limits) and to reduce peak GPU memory
  // usage (avoids OOM when the aggregation working set is large).
  std::vector<std::unique_ptr<cudf::table>> batches;
  try {
    batches = getConcatenatedTableBatched(inputs_, inputType_, stream);
  } catch (const std::bad_alloc& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]: GPU OOM concatenating inputs: {}",
        planNodeId(),
        e.what());
  }
  inputs_.clear();

  if (noMoreInput_) {
    finished_ = true;
  }

  VELOX_CHECK(!batches.empty());

  // Check whether incremental merge is possible for group-by aggregation:
  // requires non-global, non-distinct, and all intermediate aggregators valid.
  bool canMergeGroupBy = batches.size() > 1 && !isGlobal_ && !isDistinct_;
  if (canMergeGroupBy) {
    for (const auto& agg : intermediateAggregators_) {
      if (!agg) {
        canMergeGroupBy = false;
        break;
      }
    }
  }

  bool canMergeDistinct = batches.size() > 1 && isDistinct_;

  try {
    // --- Incremental group-by aggregation across multiple batches ---
    if (canMergeGroupBy) {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                << " incremental group-by over " << batches.size() << " batches";

      CudfVectorPtr mergedResult;
      for (size_t bi = 0; bi < batches.size(); ++bi) {
        auto& batch = batches[bi];
        if (!batch || batch->num_rows() == 0) continue;

        auto batchResult = doGroupByAggregation(
            batch->view(), groupingKeyInputChannels_, aggregators_, stream);
        batch.reset();

        if (!batchResult) continue;

        if (!mergedResult) {
          mergedResult = std::move(batchResult);
        } else {
          std::vector<cudf::table_view> toConcat;
          toConcat.push_back(mergedResult->getTableView());
          toConcat.push_back(batchResult->getTableView());

          std::vector<AlignedTable> mergeAlignStorage;
          for (size_t ci = 0; ci < toConcat.size(); ++ci) {
            if (hasDecimal128Misalignment(toConcat[ci])) {
              mergeAlignStorage.push_back(
                  ensureDecimal128Alignment(toConcat[ci], stream));
              toConcat[ci] = mergeAlignStorage.back().view;
            }
          }

          std::unique_ptr<cudf::table> concatTable;
          try {
            concatTable = cudf::concatenate(
                toConcat, stream, cudf::get_current_device_resource_ref());
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: merge concat failed (" << e.what()
                         << "), syncing and retrying";
            cudaDeviceSynchronize();
            cudaGetLastError();
            concatTable = cudf::concatenate(
                toConcat, stream, cudf::get_current_device_resource_ref());
          }
          batchResult.reset();

          mergedResult = doGroupByAggregation(
              concatTable->view(), groupingKeyOutputChannels_,
              intermediateAggregators_, stream);
        }
      }
      batches.clear();

      if (mergedResult) {
        VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                  << " incremental aggregation produced "
                  << mergedResult->size() << " rows";
      }
      return mergedResult;
    }

    // --- Incremental distinct across multiple batches ---
    if (canMergeDistinct) {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                << " incremental distinct over " << batches.size() << " batches";

      CudfVectorPtr mergedResult;
      for (size_t bi = 0; bi < batches.size(); ++bi) {
        auto& batch = batches[bi];
        if (!batch || batch->num_rows() == 0) continue;

        auto batchResult = getDistinctKeys(
            batch->view(), groupingKeyInputChannels_, stream);
        batch.reset();

        if (!batchResult) continue;

        if (!mergedResult) {
          mergedResult = std::move(batchResult);
        } else {
          std::vector<cudf::table_view> toConcat;
          toConcat.push_back(mergedResult->getTableView());
          toConcat.push_back(batchResult->getTableView());

          std::vector<AlignedTable> distinctMergeAlignStorage;
          for (size_t ci = 0; ci < toConcat.size(); ++ci) {
            if (hasDecimal128Misalignment(toConcat[ci])) {
              distinctMergeAlignStorage.push_back(
                  ensureDecimal128Alignment(toConcat[ci], stream));
              toConcat[ci] = distinctMergeAlignStorage.back().view;
            }
          }

          std::unique_ptr<cudf::table> concatTable;
          try {
            concatTable = cudf::concatenate(
                toConcat, stream, cudf::get_current_device_resource_ref());
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: distinct merge concat failed (" << e.what()
                         << "), syncing and retrying";
            cudaDeviceSynchronize();
            cudaGetLastError();
            concatTable = cudf::concatenate(
                toConcat, stream, cudf::get_current_device_resource_ref());
          }
          batchResult.reset();

          mergedResult = getDistinctKeys(
              concatTable->view(), groupingKeyOutputChannels_, stream);
        }
      }
      batches.clear();

      if (mergedResult) {
        VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                  << " incremental distinct produced "
                  << mergedResult->size() << " rows";
      }
      return mergedResult;
    }

    // --- Single batch or global fallback ---
    std::unique_ptr<cudf::table> tbl;
    if (batches.size() == 1) {
      tbl = std::move(batches[0]);
    } else {
      // Pairwise binary reduction: concatenate pairs of batches iteratively
      // instead of all-at-once. This limits peak GPU memory (only 2 inputs +
      // 1 output at each step) and avoids cudaErrorInvalidValue when many
      // small batches are passed to cudf::concatenate simultaneously.
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                   << " pairwise concat for " << batches.size() << " batches";
      while (batches.size() > 1) {
        std::vector<std::unique_ptr<cudf::table>> nextLevel;
        nextLevel.reserve((batches.size() + 1) / 2);
        for (size_t i = 0; i < batches.size(); i += 2) {
          if (i + 1 >= batches.size()) {
            nextLevel.push_back(std::move(batches[i]));
            continue;
          }
          std::vector<cudf::table_view> views = {
              batches[i]->view(), batches[i + 1]->view()};

          std::vector<AlignedTable> pairAlignStorage;
          for (size_t vi = 0; vi < views.size(); ++vi) {
            if (hasDecimal128Misalignment(views[vi])) {
              pairAlignStorage.push_back(
                  ensureDecimal128Alignment(views[vi], stream));
              views[vi] = pairAlignStorage.back().view;
            }
          }

          std::unique_ptr<cudf::table> merged;
          try {
            merged = cudf::concatenate(
                views, stream, cudf::get_current_device_resource_ref());
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: pairwise concat failed at [" << i << ","
                         << (i + 1) << "]: " << e.what()
                         << ". Syncing and retrying.";
            cudaDeviceSynchronize();
            cudaGetLastError();
            merged = cudf::concatenate(
                views, stream, cudf::get_current_device_resource_ref());
          }
          batches[i].reset();
          batches[i + 1].reset();
          nextLevel.push_back(std::move(merged));
        }
        batches = std::move(nextLevel);
      }
      tbl = std::move(batches[0]);
    }

    VELOX_CHECK_NOT_NULL(tbl);

    VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
              << " concatenatedRows=" << tbl->num_rows()
              << " concatenatedCols=" << tbl->num_columns();

    if (tbl->num_rows() == 0 && !isGlobal_) {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                   << " concatenated table has 0 rows (non-global), returning nullptr";
      return nullptr;
    }

    // Run the aggregation with OOM recovery. On OOM:
    //  1. Sync GPU to free pending async allocations and retry.
    //  2. If that still fails, split the table in half and process each half
    //     separately. For distinct this is always correct; for group-by it
    //     is correct when using the same aggregators (sum/min/max/count).
    auto runAggregation = [&](cudf::table_view view) -> CudfVectorPtr {
      if (isDistinct_) {
        return getDistinctKeys(view, groupingKeyInputChannels_, stream);
      } else if (isGlobal_) {
        return doGlobalAggregation(view, stream);
      } else {
        return doGroupByAggregation(
            view, groupingKeyInputChannels_, aggregators_, stream);
      }
    };

    // Determine if split-merge is possible: distinct is always splittable;
    // group-by requires valid intermediate aggregators for the merge step;
    // global aggregation cannot be split.
    bool canSplitOnOOM = !isGlobal_ && tbl->num_rows() > 1000;
    if (canSplitOnOOM && !isDistinct_) {
      for (const auto& agg : intermediateAggregators_) {
        if (!agg) { canSplitOnOOM = false; break; }
      }
    }

    // Reclaim cached GPU memory from the CUDA async pool. Called before
    // retries and split-merge attempts so fragmented free blocks are
    // consolidated and available for new allocations.
    auto trimGpuMemoryPool = [&]() {
      cudaDeviceSynchronize();
      cudaGetLastError();
      int device = 0;
      cudaGetDevice(&device);
      cudaMemPool_t pool = nullptr;
      if (cudaDeviceGetDefaultMemPool(&pool, device) == cudaSuccess &&
          pool != nullptr) {
        cudaMemPoolTrimTo(pool, 0);
      }
    };

    // Split the table into N chunks, run aggregation on each chunk
    // sequentially (releasing intermediate hash tables between chunks),
    // then merge all results. Adaptive: starts with 2 chunks, doubles
    // on failure up to kMaxSplitParts. Returns true on success.
    auto trySplitMerge = [&](CudfVectorPtr& out) -> bool {
      static constexpr int kMaxSplitParts = 16;
      int numParts = 2;

      while (numParts <= kMaxSplitParts) {
        auto numRows = tbl->num_rows();
        std::vector<cudf::size_type> splitIndices;
        for (int p = 1; p < numParts; ++p) {
          splitIndices.push_back(
              static_cast<cudf::size_type>((int64_t)numRows * p / numParts));
        }
        auto parts = cudf::split(tbl->view(), splitIndices, stream);

        LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                     << "]: split-merge " << numRows << " rows into "
                     << numParts << " parts (first="
                     << parts[0].num_rows() << ")";

        std::vector<CudfVectorPtr> chunkResults;
        bool allChunksOk = true;
        for (size_t ci = 0; ci < parts.size(); ++ci) {
          trimGpuMemoryPool();
          try {
            auto chunkResult = runAggregation(parts[ci]);
            if (chunkResult) {
              chunkResults.push_back(std::move(chunkResult));
            }
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: split-merge chunk " << ci << "/"
                         << parts.size() << " failed: " << e.what();
            allChunksOk = false;
            cudaDeviceSynchronize();
            cudaGetLastError();
            break;
          }
        }

        if (!allChunksOk) {
          chunkResults.clear();
          numParts *= 2;
          LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                       << "]: retrying with " << numParts << " parts";
          continue;
        }

        stream.synchronize();
        tbl.reset();

        if (chunkResults.empty()) {
          out = nullptr;
          return true;
        }
        if (chunkResults.size() == 1) {
          out = std::move(chunkResults[0]);
          return true;
        }

        std::vector<cudf::table_view> toConcat;
        for (auto& cr : chunkResults) {
          toConcat.push_back(cr->getTableView());
        }
        auto mergedTable = cudf::concatenate(
            toConcat, stream, cudf::get_current_device_resource_ref());
        chunkResults.clear();

        trimGpuMemoryPool();
        if (isDistinct_) {
          out = getDistinctKeys(
              mergedTable->view(), groupingKeyOutputChannels_, stream);
        } else {
          out = doGroupByAggregation(
              mergedTable->view(), groupingKeyOutputChannels_,
              intermediateAggregators_, stream);
        }
        return true;
      }
      return false;
    };

    // OOM recovery strategy:
    //  1. Trim GPU memory pool before first attempt to reclaim fragmented
    //     memory from prior operators.
    //  2. Retry with exponential backoff (other GPU operators on separate
    //     threads release memory as they complete).
    //  3. After kSplitAttemptRetry failures, split-and-merge to halve peak
    //     GPU memory per aggregation step.
    //  4. Give up after kMaxOomRetries.
    CudfVectorPtr result;
    static constexpr int kMaxOomRetries = 20;
    static constexpr int kInitialBackoffMs = 200;
    static constexpr int kMaxBackoffMs = 5000;
    static constexpr int kSplitAttemptRetry = 3;

    trimGpuMemoryPool();

    for (int oomRetry = 0;; ++oomRetry) {
      try {
        cudaDeviceSynchronize();
        cudaGetLastError();
        result = runAggregation(tbl->view());
        break;
      } catch (const std::bad_alloc& oom) {
        cudaDeviceSynchronize();
        cudaGetLastError();

        if (oomRetry >= kMaxOomRetries) {
          LOG(ERROR) << "CudfHashAggregation[" << planNodeId()
                     << "]: OOM after " << kMaxOomRetries
                     << " retries, giving up: " << oom.what();
          throw;
        }

        if (oomRetry == kSplitAttemptRetry && canSplitOnOOM) {
          try {
            if (trySplitMerge(result)) {
              LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                           << "]: split-merge succeeded, produced "
                           << (result ? result->size() : 0) << " rows";
              break;
            }
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: split-merge failed: " << e.what()
                         << ", continuing retries";
            cudaDeviceSynchronize();
            cudaGetLastError();
            if (!tbl) throw;
          }
        }

        trimGpuMemoryPool();
        size_t freeMem = 0, totalMem = 0;
        cudaMemGetInfo(&freeMem, &totalMem);
        int backoffMs = std::min(
            kMaxBackoffMs, kInitialBackoffMs * (1 << std::min(oomRetry, 5)));

        LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                     << "]: OOM retry " << (oomRetry + 1) << "/"
                     << kMaxOomRetries << " (" << oom.what()
                     << "), free=" << (freeMem >> 20)
                     << "MB, waiting " << backoffMs << "ms";

        std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
      } catch (const cudf::cuda_error& ce) {
        cudaDeviceSynchronize();
        cudaGetLastError();

        if (oomRetry >= kMaxOomRetries) {
          LOG(ERROR) << "CudfHashAggregation[" << planNodeId()
                     << "]: CUDA error after " << kMaxOomRetries
                     << " retries: " << ce.what();
          throw;
        }

        if (oomRetry == kSplitAttemptRetry && canSplitOnOOM) {
          try {
            if (trySplitMerge(result)) {
              LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                           << "]: split-merge (CUDA error recovery) succeeded";
              break;
            }
          } catch (const std::exception& e) {
            LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                         << "]: split-merge (CUDA error recovery) failed: "
                         << e.what() << ", continuing retries";
            cudaDeviceSynchronize();
            cudaGetLastError();
            if (!tbl) throw;
          }
        }

        int backoffMs = std::min(
            kMaxBackoffMs, kInitialBackoffMs * (1 << std::min(oomRetry, 5)));
        LOG(WARNING) << "CudfHashAggregation[" << planNodeId()
                     << "]: CUDA error retry " << (oomRetry + 1) << "/"
                     << kMaxOomRetries << " (" << ce.what()
                     << "), waiting " << backoffMs << "ms";
        std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
      }
    }

    if (!result) {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                   << " aggregation returned nullptr despite "
                   << tbl->num_rows() << " input rows"
                   << " (global=" << isGlobal_
                   << " distinct=" << isDistinct_ << ")";
    } else {
      VLOG(1) << "[DIAG] getOutput node=" << planNodeId()
                << " aggregation produced " << result->size() << " rows";
    }
    return result;
  } catch (const std::bad_alloc& e) {
    VELOX_FAIL(
        "CudfHashAggregation[{}]: GPU OOM in aggregation: {}",
        planNodeId(),
        e.what());
  } catch (const cudf::cuda_error& e) {
    cudaGetLastError();
    VELOX_FAIL(
        "CudfHashAggregation[{}]: CUDA error in aggregation: {}",
        planNodeId(),
        e.what());
  }
}

void CudfHashAggregation::noMoreInput() {
  Operator::noMoreInput();
  if (isPartialOutput_ && !isGlobal_) {
    GpuGuard gpuGuard;
    processAccumulatedPartialInputs();
  }
  if (isPartialOutput_ && inputs_.empty() &&
      accumulatedPartialInputs_.empty()) {
    finished_ = true;
  }
}

bool CudfHashAggregation::isFinished() {
  return finished_ && accumulatedPartialInputs_.empty();
}

// Step-aware aggregation registry implementation
StepAwareAggregationRegistry& getStepAwareAggregationRegistry() {
  static StepAwareAggregationRegistry registry;
  return registry;
}

bool registerAggregationFunctionForStep(
    const std::string& name,
    core::AggregationNode::Step step,
    const std::vector<exec::FunctionSignaturePtr>& signatures,
    bool overwrite) {
  auto& registry = getStepAwareAggregationRegistry();

  if (!overwrite && registry.find(name) != registry.end() &&
      registry[name].find(step) != registry[name].end()) {
    return false;
  }

  registry[name][step] = signatures;
  return true;
}

// Register step-aware builtin aggregation functions
bool registerStepAwareBuiltinAggregationFunctions(const std::string& prefix) {
  using exec::FunctionSignatureBuilder;

  // Register sum function (split by aggregation step)
  auto sumSingleSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("tinyint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("real")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build()};

  // Decimal sum signatures.
  auto decimalSumSingle = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("decimal(38, a_scale)")
          .argumentType("decimal(a_precision, a_scale)")
          .build()};
  auto decimalSumPartial = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("varbinary")
          .argumentType("decimal(a_precision, a_scale)")
          .build()};
  auto decimalSumFinal = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_scale")
          .returnType("decimal(38, a_scale)")
          .argumentType("varbinary")
          .build(),
      // After shuffle, intermediate state may arrive as ROW(sum, count)
      // instead of serialized varbinary.
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("decimal(38, a_scale)")
          .argumentType("row(decimal(a_precision, a_scale),bigint)")
          .build()};
  auto decimalSumIntermediate =
      std::vector<exec::FunctionSignaturePtr>{
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("varbinary")
              .build(),
          FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("row(decimal(a_precision, a_scale),bigint)")
              .argumentType("row(decimal(a_precision, a_scale),bigint)")
              .build()};

  sumSingleSignatures.insert(
      sumSingleSignatures.end(),
      decimalSumSingle.begin(),
      decimalSumSingle.end());

  registerAggregationFunctionForStep(
      prefix + "sum",
      core::AggregationNode::Step::kSingle,
      sumSingleSignatures);

  auto sumPartialSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("tinyint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build()};

  sumPartialSignatures.insert(
      sumPartialSignatures.end(),
      decimalSumPartial.begin(),
      decimalSumPartial.end());

  registerAggregationFunctionForStep(
      prefix + "sum",
      core::AggregationNode::Step::kPartial,
      sumPartialSignatures);

  auto sumFinalIntermediateSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build()};

  auto sumFinalSignatures = sumFinalIntermediateSignatures;
  sumFinalSignatures.insert(
      sumFinalSignatures.end(), decimalSumFinal.begin(), decimalSumFinal.end());

  registerAggregationFunctionForStep(
      prefix + "sum", core::AggregationNode::Step::kFinal, sumFinalSignatures);

  auto sumIntermediateSignatures = sumFinalIntermediateSignatures;
  sumIntermediateSignatures.insert(
      sumIntermediateSignatures.end(),
      decimalSumIntermediate.begin(),
      decimalSumIntermediate.end());

  registerAggregationFunctionForStep(
      prefix + "sum",
      core::AggregationNode::Step::kIntermediate,
      sumIntermediateSignatures);

  // Register count function (split by aggregation step)
  auto countSinglePartialSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("tinyint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("double")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("varchar")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("boolean")
          .build(),
      FunctionSignatureBuilder()
          .integerVariable("p")
          .integerVariable("s")
          .returnType("bigint")
          .argumentType("decimal(p,s)")
          .build(),
      FunctionSignatureBuilder().returnType("bigint").build()};

  registerAggregationFunctionForStep(
      prefix + "count",
      core::AggregationNode::Step::kSingle,
      countSinglePartialSignatures);
  registerAggregationFunctionForStep(
      prefix + "count",
      core::AggregationNode::Step::kPartial,
      countSinglePartialSignatures);

  auto countFinalIntermediateSignatures =
      std::vector<exec::FunctionSignaturePtr>{FunctionSignatureBuilder()
                                                  .returnType("bigint")
                                                  .argumentType("bigint")
                                                  .build()};
  registerAggregationFunctionForStep(
      prefix + "count",
      core::AggregationNode::Step::kFinal,
      countFinalIntermediateSignatures);
  registerAggregationFunctionForStep(
      prefix + "count",
      core::AggregationNode::Step::kIntermediate,
      countFinalIntermediateSignatures);

  // Register min function (same signatures for all steps)
  auto minMaxSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("tinyint")
          .argumentType("tinyint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("smallint")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("integer")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("real")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build(),
      FunctionSignatureBuilder()
          .integerVariable("p")
          .integerVariable("s")
          .returnType("decimal(p,s)")
          .argumentType("decimal(p,s)")
          .build()};

  registerAggregationFunctionForStep(
      prefix + "min", core::AggregationNode::Step::kSingle, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "min", core::AggregationNode::Step::kPartial, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "min", core::AggregationNode::Step::kFinal, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "min",
      core::AggregationNode::Step::kIntermediate,
      minMaxSignatures);

  // Register max function (same signatures for all steps)
  registerAggregationFunctionForStep(
      prefix + "max", core::AggregationNode::Step::kSingle, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "max", core::AggregationNode::Step::kPartial, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "max", core::AggregationNode::Step::kFinal, minMaxSignatures);
  registerAggregationFunctionForStep(
      prefix + "max",
      core::AggregationNode::Step::kIntermediate,
      minMaxSignatures);

  // Register avg function (different signatures for different steps)

  // Single step: avg(input_type) -> double
  auto avgSingleSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("real")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build()};

  // Decimal avg signatures.
  auto decimalAvgSingle = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("decimal(a_precision, a_scale)")
          .argumentType("decimal(a_precision, a_scale)")
          .build()};
  auto decimalAvgPartial = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("varbinary")
          .argumentType("decimal(a_precision, a_scale)")
          .build()};
  auto decimalAvgFinal = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("decimal(a_precision, a_scale)")
          .argumentType("varbinary")
          .build(),
      // After shuffle, intermediate state may arrive as ROW(sum, count)
      // instead of serialized varbinary.
      FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType("decimal(a_precision, a_scale)")
          .argumentType("row(decimal(a_precision, a_scale),bigint)")
          .build()};
  auto decimalAvgIntermediate =
      std::vector<exec::FunctionSignaturePtr>{
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("varbinary")
              .build(),
          FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("row(decimal(a_precision, a_scale),bigint)")
              .argumentType("row(decimal(a_precision, a_scale),bigint)")
              .build()};

  avgSingleSignatures.insert(
      avgSingleSignatures.end(),
      decimalAvgSingle.begin(),
      decimalAvgSingle.end());

  registerAggregationFunctionForStep(
      prefix + "avg",
      core::AggregationNode::Step::kSingle,
      avgSingleSignatures);

  // Partial step: avg(input_type) -> row(sum input_type, count bigint)
  auto avgPartialSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("double")
          .build()};

  avgPartialSignatures.insert(
      avgPartialSignatures.end(),
      decimalAvgPartial.begin(),
      decimalAvgPartial.end());

  registerAggregationFunctionForStep(
      prefix + "avg",
      core::AggregationNode::Step::kPartial,
      avgPartialSignatures);

  // Final step: avg(row(double, bigint)) -> double
  auto avgFinalIntermediateSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("row(double,bigint)")
          .build()};

  auto avgFinalSignatures = avgFinalIntermediateSignatures;
  avgFinalSignatures.insert(
      avgFinalSignatures.end(), decimalAvgFinal.begin(), decimalAvgFinal.end());

  registerAggregationFunctionForStep(
      prefix + "avg", core::AggregationNode::Step::kFinal, avgFinalSignatures);

  // Intermediate step: avg(row(sum input_type, count bigint)) -> row(sum
  // input_type, count bigint)
  auto avgIntermediateSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("row(double,bigint)")
          .argumentType("row(double,bigint)")
          .build()};

  // WHY DOES SUM NOT HAVE THE EQUIVALENT OF THE ABOVE?
  // THE ABOVE THEN CLASHES WITH BELOW
  // @mattgara HELP! :)
  // auto avgIntermediateSignatures = avgFinalIntermediateSignatures;
  avgIntermediateSignatures.insert(
      avgIntermediateSignatures.end(),
      decimalAvgIntermediate.begin(),
      decimalAvgIntermediate.end());

  registerAggregationFunctionForStep(
      prefix + "avg",
      core::AggregationNode::Step::kIntermediate,
      avgIntermediateSignatures);

  // Register approx_distinct function
  auto approxDistinctSingleSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("tinyint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("smallint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("integer")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("real")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("double")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("varchar")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("varbinary")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("date")
          .build(),
      FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("timestamp")
          .build()};
  registerAggregationFunctionForStep(
      prefix + "approx_distinct",
      core::AggregationNode::Step::kSingle,
      approxDistinctSingleSignatures);

  auto approxDistinctPartialSignatures =
      std::vector<exec::FunctionSignaturePtr>{
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("tinyint")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("smallint")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("integer")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("bigint")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("real")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("double")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("varchar")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("varbinary")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("date")
              .build(),
          FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("timestamp")
              .build()};
  registerAggregationFunctionForStep(
      prefix + "approx_distinct",
      core::AggregationNode::Step::kPartial,
      approxDistinctPartialSignatures);

  auto approxDistinctIntermediateSignatures =
      std::vector<exec::FunctionSignaturePtr>{FunctionSignatureBuilder()
                                                  .returnType("varbinary")
                                                  .argumentType("varbinary")
                                                  .build()};
  registerAggregationFunctionForStep(
      prefix + "approx_distinct",
      core::AggregationNode::Step::kIntermediate,
      approxDistinctIntermediateSignatures);

  auto approxDistinctFinalSignatures =
      std::vector<exec::FunctionSignaturePtr>{FunctionSignatureBuilder()
                                                  .returnType("bigint")
                                                  .argumentType("varbinary")
                                                  .build()};
  registerAggregationFunctionForStep(
      prefix + "approx_distinct",
      core::AggregationNode::Step::kFinal,
      approxDistinctFinalSignatures);

  // Register bloom_filter_agg (3-arg, 2-arg, and 1-arg variants)
  auto bloomPartialSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("varbinary")
          .argumentType("bigint")
          .constantArgumentType("bigint")
          .constantArgumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("varbinary")
          .argumentType("bigint")
          .constantArgumentType("bigint")
          .build(),
      FunctionSignatureBuilder()
          .returnType("varbinary")
          .argumentType("bigint")
          .build()};
  registerAggregationFunctionForStep(
      prefix + "bloom_filter_agg",
      core::AggregationNode::Step::kPartial,
      bloomPartialSignatures);
  registerAggregationFunctionForStep(
      prefix + "bloom_filter_agg",
      core::AggregationNode::Step::kSingle,
      bloomPartialSignatures);

  auto bloomMergeSignatures =
      std::vector<exec::FunctionSignaturePtr>{FunctionSignatureBuilder()
                                                  .returnType("varbinary")
                                                  .argumentType("varbinary")
                                                  .build()};
  registerAggregationFunctionForStep(
      prefix + "bloom_filter_agg",
      core::AggregationNode::Step::kIntermediate,
      bloomMergeSignatures);
  registerAggregationFunctionForStep(
      prefix + "bloom_filter_agg",
      core::AggregationNode::Step::kFinal,
      bloomMergeSignatures);

  // Register stddev / stddev_samp / variance / var_samp
  // Velox Spark intermediate type: row(bigint, double, double) = (count, mean, m2)
  auto varianceSingleSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("double")
          .build()};
  auto variancePartialSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("row(bigint,double,double)")
          .argumentType("double")
          .build()};
  auto varianceFinalSignatures = std::vector<exec::FunctionSignaturePtr>{
      FunctionSignatureBuilder()
          .returnType("double")
          .argumentType("row(bigint,double,double)")
          .build()};
  auto varianceIntermediateSignatures =
      std::vector<exec::FunctionSignaturePtr>{
          FunctionSignatureBuilder()
              .returnType("row(bigint,double,double)")
              .argumentType("row(bigint,double,double)")
              .build()};

  for (auto const& name :
       {prefix + "stddev", prefix + "stddev_samp",
        prefix + "variance", prefix + "var_samp"}) {
    registerAggregationFunctionForStep(
        name,
        core::AggregationNode::Step::kSingle,
        varianceSingleSignatures);
    registerAggregationFunctionForStep(
        name,
        core::AggregationNode::Step::kPartial,
        variancePartialSignatures);
    registerAggregationFunctionForStep(
        name,
        core::AggregationNode::Step::kFinal,
        varianceFinalSignatures);
    registerAggregationFunctionForStep(
        name,
        core::AggregationNode::Step::kIntermediate,
        varianceIntermediateSignatures);
  }

  return true;
}

bool matchTypedCallAgainstSignatures(
    const core::CallTypedExpr& call,
    const std::vector<exec::FunctionSignaturePtr>& sigs) {
  const auto n = call.inputs().size();
  std::vector<TypePtr> argTypes;
  argTypes.reserve(n);
  for (const auto& input : call.inputs()) {
    argTypes.push_back(input->type());
  }
  for (const auto& sig : sigs) {
    std::vector<Coercion> coercions(n);
    exec::SignatureBinder binder(*sig, argTypes);
    if (!binder.tryBindWithCoercions(coercions)) {
      continue;
    }

    return true;
  }
  return false;
}

// Step-aware aggregation validation function
bool canAggregationBeEvaluatedByCudf(
    const core::CallTypedExpr& call,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes,
    core::QueryCtx* queryCtx) {
  auto& stepAwareRegistry = getStepAwareAggregationRegistry();
  auto originalName = getOriginalName(call.name());
  auto companionStep = getCompanionStep(call.name(), step);

  auto funcIt = stepAwareRegistry.find(originalName);
  if (funcIt == stepAwareRegistry.end()) {
    return false;
  }

  auto stepIt = funcIt->second.find(companionStep);
  if (stepIt != funcIt->second.end()) {
    if (matchTypedCallAgainstSignatures(call, stepIt->second)) {
      return true;
    }
  }

  // Allow decimal avg/sum kFinal/kIntermediate with ROW(DECIMAL, BIGINT)
  if ((companionStep == core::AggregationNode::Step::kFinal ||
       companionStep == core::AggregationNode::Step::kIntermediate) &&
      (originalName.size() >= 3 &&
       (originalName.substr(originalName.size() - 3) == "avg" ||
        originalName.substr(originalName.size() - 3) == "sum"))) {
    if (!call.inputs().empty() && call.inputs()[0]->type()->isRow()) {
      auto const& rowType = call.inputs()[0]->type()->asRow();
      if (rowType.size() >= 1 && rowType.childAt(0)->isDecimal()) {
        return true;
      }
    }
  }

  // For _merge_extract companions in a SINGLE-step node, try the other
  // candidate step if the first didn't match.
  if (call.name().find("_merge_extract") != std::string::npos &&
      step == core::AggregationNode::Step::kSingle) {
    auto altStep = (companionStep == core::AggregationNode::Step::kSingle)
        ? core::AggregationNode::Step::kFinal
        : core::AggregationNode::Step::kSingle;
    auto altIt = funcIt->second.find(altStep);
    if (altIt != funcIt->second.end()) {
      if (matchTypedCallAgainstSignatures(call, altIt->second)) {
        return true;
      }
    }
  }

  return false;
}

bool canBeEvaluatedByCudf(
    const core::AggregationNode& aggregationNode,
    core::QueryCtx* queryCtx) {
  const core::PlanNode* sourceNode = aggregationNode.sources().empty()
      ? nullptr
      : aggregationNode.sources()[0].get();

  // Get the aggregation step from the node
  auto step = aggregationNode.step();

  // Check supported aggregation functions using step-aware aggregation registry
  for (const auto& aggregate : aggregationNode.aggregates()) {
    // Use step-aware validation that handles partial/final/intermediate steps
    if (!canAggregationBeEvaluatedByCudf(
            *aggregate.call, step, aggregate.rawInputTypes, queryCtx)) {
      return false;
    }

    // `distinct` aggregations are not supported, in testing fails with "De-dup
    // before aggregation is not yet supported"
    if (aggregate.distinct) {
      return false;
    }

    // `mask` is NOT supported (in testing do not appear to be be applied and
    // return incorrect results )
    if (aggregate.mask) {
      return false;
    }

    // Check input expressions can be evaluated by CUDF, expand the input first
    for (const auto& input : aggregate.call->inputs()) {
      auto expandedInput = expandFieldReference(input, sourceNode);
      std::vector<core::TypedExprPtr> exprs = {expandedInput};
      if (!canBeEvaluatedByCudf(exprs, queryCtx)) {
        return false;
      }
    }
  }

  // Check grouping key expressions
  if (!canGroupingKeysBeEvaluatedByCudf(
          aggregationNode.groupingKeys(), sourceNode, queryCtx)) {
    return false;
  }

  return true;
}

core::TypedExprPtr expandFieldReference(
    const core::TypedExprPtr& expr,
    const core::PlanNode* sourceNode) {
  // If this is a field reference and we have a source projection, expand it
  if (expr->kind() == core::ExprKind::kFieldAccess && sourceNode) {
    auto projectNode = dynamic_cast<const core::ProjectNode*>(sourceNode);
    if (projectNode) {
      auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr);
      if (fieldExpr) {
        // Find the corresponding projection expression
        const auto& projections = projectNode->projections();
        const auto& names = projectNode->names();
        for (size_t i = 0; i < names.size(); ++i) {
          if (names[i] == fieldExpr->name()) {
            return projections[i];
          }
        }
      }
    }
  }
  return expr;
}

bool canGroupingKeysBeEvaluatedByCudf(
    const std::vector<core::FieldAccessTypedExprPtr>& groupingKeys,
    const core::PlanNode* sourceNode,
    core::QueryCtx* queryCtx) {
  // Check grouping key expressions (with expansion)
  for (const auto& groupingKey : groupingKeys) {
    auto expandedKey = expandFieldReference(groupingKey, sourceNode);
    std::vector<core::TypedExprPtr> exprs = {expandedKey};
    if (!canBeEvaluatedByCudf(exprs, queryCtx)) {
      return false;
    }
  }

  return true;
}

} // namespace facebook::velox::cudf_velox
