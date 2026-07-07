# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(
  ucx_exchange_test
  UcxOutputQueueManagerTest.cpp
  UcxOutputQueueLifecycleTest.cpp
  UcxExchangeProtocolTest.cpp
  UcxExchangeTest.cpp
  IntraNodeTransferRegistryTest.cpp
  UcxTaskLifecycleRegistryTest.cpp
  Main.cpp
  UcxTestHelpers.cpp
  UcxTestData.cpp
  UcxPartitionedOutputMock.cpp
  SinkDriverMock.cpp
  SourceDriverMock.cpp
)

add_test(
  NAME ucx_exchange_test
  COMMAND ucx_exchange_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set_tests_properties(ucx_exchange_test PROPERTIES LABELS cuda_driver TIMEOUT 3000)

target_link_libraries(
  ucx_exchange_test
  velox_ucx_exchange
  velox_cudf_exec
  velox_exec
  velox_exec_test_lib
  velox_test_util
  velox_vector_fuzzer
  gtest
  gtest_main
  Folly::folly
  fmt::fmt
  ucxx::ucxx
)

# GCC toolset installations on CentOS can expose a newer compiler through an
# older toolset path while CMake also injects an older system libstdc++ search
# directory. Newer inline ABI helpers then live in libstdc++_nonshared but are
# not selected by the generated link line. Ask the active compiler for its
# matching compatibility archive instead of hard-coding a toolset version.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libstdc++_nonshared.a
    OUTPUT_VARIABLE LIBSTDCPP_NONSHARED
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(EXISTS "${LIBSTDCPP_NONSHARED}")
    # Pull every compatibility definition into the executable.  A plain
    # archive entry can be emitted before transitive shared libraries (such as
    # libcudf), in which case the linker has not seen their unresolved symbols
    # yet and skips the required objects.
    target_link_libraries(
      ucx_exchange_test
      "-Wl,--whole-archive"
      "${LIBSTDCPP_NONSHARED}"
      "-Wl,--no-whole-archive"
    )
  endif()
endif()
