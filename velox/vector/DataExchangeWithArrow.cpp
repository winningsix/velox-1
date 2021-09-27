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

/**
 *
 * Unitills for zero-copy data transform with Arrow::array
 *
 **/
#include "velox/vector/DataExchangeWithArrow.h"
#include "velox/vector/FlatVector.h"

// TODEL:
#include <iostream>

namespace facebook {
namespace velox {

// Reference:https://arrow.apache.org/docs/format/CDataInterface.html#exporting-a-struct-float32-utf8-array
std::pair<ArrowArray*, ArrowSchema*> DataUtil::fromVeloxToArrow(
    const RowVectorPtr& row_vector_ptr) {
  // for now we only focus on primitive type
  ArrowArray* array = new ArrowArray;
  ArrowSchema* schema = new ArrowSchema;
  // Note: Make sure construct schema first and then array. Since after
  // export_from_velox(), the object could not be accessed.
  export_schema_from_Velox_type(row_vector_ptr, schema);
  export_from_velox(row_vector_ptr, array);
  return std::make_pair(array, schema);
}

// Callback for ArrowArray to delete children ArrowArray, and free private_data
void DataUtil::release_malloced_array(struct ArrowArray* array) {
  int i;
  // Free children, call children's release
  for (i = 0; i < array->n_children; ++i) {
    struct ArrowArray* child = array->children[i];
    if (child->release != nullptr) {
      child->release(child);
    }
  }
  std::free(array->children);
  // Free original phisical buffer. TODO:test
  delete array->private_data;
  // Mark released
  array->release = nullptr;
}

// Take RowVectorPtr as input, construct ArrowArray as output
void DataUtil::export_from_velox(
    const RowVectorPtr& row_vector_ptr,
    struct ArrowArray* array) {
  struct ArrowArray* child;
  //
  // Initialize parent array
  //
  size_t children_size = row_vector_ptr->childrenSize();
  *array = (struct ArrowArray){
      // Data description
      .length = row_vector_ptr->size(), // VERIFY: number of items? what if
                                        // child has different nitems?
      .null_count = -1, // TODO: for parent, there is no null
      .offset = 0,
      .n_buffers = 1, // VERIFY: parent has one buffer
      .n_children = static_cast<int long>(children_size),
      .dictionary = nullptr,
      // Bookkeeping
      .release = &release_malloced_array};
  array->buffers = (const void**)malloc(sizeof(void*) * array->n_buffers);
  array->buffers[0] = nullptr; // no nulls, null bitmap can be omitted
  // Allocate list of children arrays
  array->children =
      (ArrowArray**)malloc(sizeof(struct ArrowArray*) * array->n_children);
  //
  // Initialize child array
  //
  for (int i = 0; i < children_size; i++) {
    VectorPtr velox_child = row_vector_ptr->childAt(i);
    int p_num = velox_child.use_count();

    child = array->children[i] = (ArrowArray*)malloc(sizeof(struct ArrowArray));
    *child = (struct ArrowArray){
        // Data description
        .length = velox_child->size(),
        .null_count = velox_child->getNullCount().value(),
        .offset = 0,
        .n_buffers =
            2, // VERIFY: only 2 buffers no matter how large a vector is?
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr, // TODO: for now only focus on primitive data
        // Bookkeeping
        .release = &release_malloced_array};

    // Assign buffer ptr (Velox::Vector.data_) to buffer ptr
    // of ArrowArray (arrow::array.buffers).
    child->buffers = (const void**)malloc(sizeof(void*) * array->n_buffers);
    // buffer[0] store is_null buffer, Note: check whether contains null or not
    if (velox_child->mayHaveNulls()) {
      BufferPtr velox_SmartNullsBufferPtr =
          velox_child->mutableNulls(velox_child->size());
      child->buffers[0] = velox_SmartNullsBufferPtr->asMutable<uint64_t>();
    } else {
      child->buffers[0] = nullptr;
    }
    // buffer[1] store value buffer
    // isReusableFlatVecto() Returns true if 'velox_child' vector is a unique
    // reference to a flat velox_child and nulls/values are uniquely referenced.
    int p_count = velox_child.use_count();
    // TODO:
    // if (BaseVector::isReusableFlatVector(velox_child)){
    switch (velox_child->typeKind()) {
      case TypeKind::BOOLEAN:
        // asMutable() return phisical buffer pointer, return type: T*
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::BOOLEAN>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::BOOLEAN>::NativeType>();
        break;
      case TypeKind::TINYINT:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::TINYINT>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::TINYINT>::NativeType>();
        break;
      case TypeKind::SMALLINT:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::SMALLINT>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::SMALLINT>::NativeType>();
        break;
      case TypeKind::INTEGER:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::INTEGER>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::INTEGER>::NativeType>();
        break;
      case TypeKind::BIGINT:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::BIGINT>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::BIGINT>::NativeType>();
        break;
      case TypeKind::REAL:
        child->buffers[1] =
            velox_child->asFlatVector<TypeTraits<TypeKind::REAL>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::REAL>::NativeType>();
        break;
      case TypeKind::DOUBLE:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::DOUBLE>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::DOUBLE>::NativeType>();
        break;
      // Below type may not work
      case TypeKind::VARCHAR:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::VARCHAR>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::VARCHAR>::NativeType>();
        break;
      case TypeKind::VARBINARY:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::VARBINARY>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::VARBINARY>::NativeType>();
        break;
      case TypeKind::TIMESTAMP:
        child->buffers[1] =
            velox_child
                ->asFlatVector<TypeTraits<TypeKind::TIMESTAMP>::NativeType>()
                ->values()
                ->asMutable<TypeTraits<TypeKind::TIMESTAMP>::NativeType>();
        break;
        // default:
        //  throw UnsupportedException();
    }
    // TODO: delete velox_buffer ptr object without free physical buffer
    // Thus make sure call export_from_Velox_type before
  }
}

// Release ArrowSchema recursively
void DataUtil::release_malloced_schema(struct ArrowSchema* schema) {
  int i;
  // call release for each child
  for (i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    if (child->release != nullptr) {
      child->release(child);
    }
  }
  std::free(schema->children);
  // Mark released
  schema->release = nullptr;
}

// Take RowVectorPtr as input, read shcema info and construct ArrowSchema
void DataUtil::export_schema_from_Velox_type(
    const RowVectorPtr& row_vector_ptr,
    struct ArrowSchema* schema) {
  struct ArrowSchema* child;
  //
  // Initialize parent type
  //
  auto rowType = row_vector_ptr->type()->asRow();
  size_t children_size = row_vector_ptr->childrenSize();
  *schema =
      (struct ArrowSchema){// Type description
                           .format = "+s",
                           .name = "",
                           .metadata = nullptr,
                           .flags = 0, // TODO
                           .n_children = static_cast<int long>(children_size),
                           .dictionary = nullptr,
                           // Bookkeeping
                           .release = &release_malloced_schema};
  // Allocate list of children types
  schema->children =
      (ArrowSchema**)malloc(sizeof(struct ArrowSchema*) * schema->n_children);
  //
  // Initialize each child type
  //
  for (int i = 0; i < schema->n_children; i++) {
    child = schema->children[i] =
        (ArrowSchema*)malloc(sizeof(struct ArrowSchema));
    *child = (struct ArrowSchema){
        // Type description
        .format = getArrowTypeByVeloxType(rowType.childAt(i)),
        // Verify: in Velox name is string, but in arrowschema name is char
        .name = rowType.nameOf(i).c_str(),
        .metadata = nullptr,
        .flags = ARROW_FLAG_NULLABLE, // TODO
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        // Bookkeeping
        .release = &release_malloced_schema};
  }
}

// TODO: may change to macro
const char* DataUtil::getArrowTypeByVeloxType(const std::shared_ptr<const Type>& type) {
  char format;
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      format = 'b';
      break;
    case TypeKind::TINYINT:
      format = 'c';
      break;
    case TypeKind::SMALLINT:
      format = 's';
      break;
    case TypeKind::INTEGER:
      format = 'i';
      break;
    case TypeKind::BIGINT:
      format = 'l';
      break;
    case TypeKind::REAL:
      format = 'f';
      break;
    case TypeKind::DOUBLE:
      format = 'g';
      break;
  }
  const char* format_ptr = &format;
  return format_ptr;
}
//
//// Work in progress
//RowVectorPtr& DataUtil::ArrowToVelox(
//    std::pair<ArrowArray*, ArrowSchema*> arrowPair) {
//  ArrowArray* arrow_array = arrowPair.first;
//  ArrowSchema* arrow_schema = arrowPair.second;
//  // No consider nested 
//  std::vector<VectorPtr> children(arrow_array->n_children);
//  for (int i = 0; i < arrow_array->n_children; i++) {
//    ArrowArray* arrow_array_child = arrow_array->children[i];
//    ArrowSchema* arrow_schema_child = arrow_schema->children[i];
//    children[i] = VELOX_DYNAMIC_ARROW_TYPE_DISPATCH(
//      ArrowArrayToVector,  //template function
//      arrow_schema_child->format,
//        );
//  }
//}
//// ref imlpliment: createScalar
//template <typename T>
//static VectorPtr DataUtil::ArrowArrayToVector() {}

} // namespace velox
} // namespace facebook
