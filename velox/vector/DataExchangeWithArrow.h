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
#include "velox/external/arrow/abi.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

/**
 * Util Class for Data Interexchange based on Arrow
 */
class DataUtil {
 public:
  // Zero-copy exchange data from Velox to Arrow
  // Input: velox::RowVectorPtr
  // Output: std::pair<ArrowArray*, ArrowSchema*>
  static std::pair<ArrowArray*, ArrowSchema*> fromVeloxToArrow(
      const RowVectorPtr& row_vector_ptr);

  // Zero-copy exchange data from Arrow to Velox
  // Input: std::pair<ArrowArray*, ArrowSchema*>
  // Output: velox::RowVectorPtr
  static velox::RowVectorPtr fromArrowToVelox(
      ArrowArray* arrow_array,
      ArrowSchema schema);

 private:
  // Util functions for VeloxToArrow
  static const char* getArrowTypeByVeloxType(const std::shared_ptr<const Type>& type);
  static void release_malloced_array(struct ArrowArray* array);
  static void export_from_velox(
      const RowVectorPtr& row_vector_ptr,
      struct ArrowArray* array);
  static void release_malloced_schema(struct ArrowSchema* schema);
  static void export_schema_from_Velox_type(
      const RowVectorPtr& row_vector_ptr,
      struct ArrowSchema* schema);
};
} // namespace facebook::velox
