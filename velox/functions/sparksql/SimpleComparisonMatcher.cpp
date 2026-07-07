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

#include "velox/functions/sparksql/SimpleComparisonMatcher.h"

#include <string_view>

#include "velox/expression/ExprConstants.h"

namespace facebook::velox::functions::sparksql {
namespace {

const core::CallTypedExpr*
asCall(const core::TypedExprPtr& expr, std::string_view name, size_t arity) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  return call != nullptr && call->name() == name &&
          call->inputs().size() == arity
      ? call
      : nullptr;
}

std::optional<int64_t> asIntegerConstant(const core::TypedExprPtr& expr) {
  const auto* constant =
      dynamic_cast<const core::ConstantTypedExpr*>(expr.get());
  if (constant == nullptr || constant->isNull()) {
    return std::nullopt;
  }

  if (constant->hasValueVector()) {
    const auto& vector = constant->valueVector();
    if (constant->type()->isBigint()) {
      return vector->as<SimpleVector<int64_t>>()->valueAt(0);
    }
    if (constant->type()->isInteger()) {
      return vector->as<SimpleVector<int32_t>>()->valueAt(0);
    }
    return std::nullopt;
  }

  if (constant->value().kind() == TypeKind::BIGINT) {
    return constant->value().value<int64_t>();
  }
  if (constant->value().kind() == TypeKind::INTEGER) {
    return constant->value().value<int32_t>();
  }
  return std::nullopt;
}

std::optional<std::string> nullCheckedInput(
    const core::TypedExprPtr& expr,
    const std::string& isNullName) {
  const auto* isNull = asCall(expr, isNullName, 1);
  if (isNull == nullptr) {
    return std::nullopt;
  }
  const auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(
      isNull->inputs().front().get());
  if (field == nullptr || !field->isInputColumn()) {
    return std::nullopt;
  }
  return field->name();
}

bool matchesBothNull(
    const core::TypedExprPtr& expr,
    const std::string& isNullName,
    const std::string& left,
    const std::string& right) {
  const auto* bothNull = asCall(expr, expression::kAnd, 2);
  if (bothNull == nullptr) {
    return false;
  }
  const auto first = nullCheckedInput(bothNull->inputs()[0], isNullName);
  const auto second = nullCheckedInput(bothNull->inputs()[1], isNullName);
  return first.has_value() && second.has_value() &&
      ((*first == left && *second == right) ||
       (*first == right && *second == left));
}

core::TypedExprPtr unwrapSparkNullsLastComparator(
    const std::string& prefix,
    const core::LambdaTypedExpr& lambda) {
  if (lambda.signature()->size() != 2) {
    return nullptr;
  }
  const auto& left = lambda.signature()->nameOf(0);
  const auto& right = lambda.signature()->nameOf(1);
  const auto isNullName = prefix + "isnull";

  // Spark materializes array_sort's default null semantics in the comparator:
  //   if (isnull(l) && isnull(r), 0,
  //     if (isnull(l), 1, if (isnull(r), -1, <value comparator>)))
  // Only unwrap this exact nulls-last contract. In particular, arbitrary
  // comparator lambdas and nulls-first comparators remain unsupported.
  const auto* bothNull = asCall(lambda.body(), expression::kIf, 3);
  if (bothNull == nullptr ||
      !matchesBothNull(bothNull->inputs()[0], isNullName, left, right) ||
      asIntegerConstant(bothNull->inputs()[1]) != 0) {
    return nullptr;
  }

  const auto* firstGuard = asCall(bothNull->inputs()[2], expression::kIf, 3);
  if (firstGuard == nullptr) {
    return nullptr;
  }
  const auto firstInput = nullCheckedInput(firstGuard->inputs()[0], isNullName);
  const auto firstResult = asIntegerConstant(firstGuard->inputs()[1]);
  const auto* secondGuard = asCall(firstGuard->inputs()[2], expression::kIf, 3);
  if (!firstInput.has_value() || !firstResult.has_value() ||
      secondGuard == nullptr) {
    return nullptr;
  }
  const auto secondInput =
      nullCheckedInput(secondGuard->inputs()[0], isNullName);
  const auto secondResult = asIntegerConstant(secondGuard->inputs()[1]);
  if (!secondInput.has_value() || !secondResult.has_value() ||
      *firstInput == *secondInput) {
    return nullptr;
  }

  const auto isNullsLastResult = [&](const std::string& input, int64_t result) {
    return (input == left && result == 1) || (input == right && result == -1);
  };
  if (!isNullsLastResult(*firstInput, *firstResult) ||
      !isNullsLastResult(*secondInput, *secondResult)) {
    return nullptr;
  }
  return secondGuard->inputs()[2];
}

} // namespace

std::optional<SimpleComparison>
SparkSimpleComparisonChecker::isSimpleComparison(
    const std::string& prefix,
    const core::LambdaTypedExpr& expr) {
  if (auto comparison =
          SimpleComparisonChecker::isSimpleComparison(prefix, expr)) {
    return comparison;
  }

  auto valueComparator = unwrapSparkNullsLastComparator(prefix, expr);
  if (valueComparator == nullptr) {
    return std::nullopt;
  }
  core::LambdaTypedExpr unwrapped(expr.signature(), valueComparator);
  return SimpleComparisonChecker::isSimpleComparison(prefix, unwrapped);
}

} // namespace facebook::velox::functions::sparksql
