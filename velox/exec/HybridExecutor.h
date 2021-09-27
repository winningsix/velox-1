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

#include <string>

#ifdef EXEC_WITH_OMNISCI
#include <Embedded/DBEngine.h>
#endif

namespace facebook::velox::exec {

/**
 * A wrapper to execution backend when enbaling Hybrid execution mode
 */
class HybridExecutor {
 public:
  static HybridExecutor* getInstance();

  std::string getBackend() {
    return backend_;
  }
  /**
   * API to create build side hash table, it's based on Arrow data format.
   * @param name for hash table
   * @param tableData for hash table data
   * @param tableSchema for hash table schema
   */
  void createTable(
      const std::string& name,
      const struct ArrowArray* tableData,
      const struct ArrowSchema* tableSchema){
      // TODO
  };

  /**
   * API to process data batch-by-batch
   * @param sql
   * @param inputData
   * @param inputSchema
   * @param outputSchema
   * @return Arrow based result
   */
  struct ArrowArray* processBlocks(
      const std::string& sql,
      const struct ArrowArray* inputData,
      const struct ArrowSchema* inputSchema,
      const struct ArrowSchema* outputSchema){
      //TODO
  };

  void cleanup() {
#ifdef EXEC_WITH_OMNISCI
    if (dbe_) {
      dbe_.reset();
    }
#endif
  };

 private:
  void init();
#ifdef EXEC_WITH_OMNISCI
  void initOminisciDBEngine();
  std::shared_ptr<EmbeddedDatabase::DBEngine> dbe_;
#endif
  static std::unique_ptr<HybridExecutor> instance_;
  static std::string backend_;
};

} // namespace facebook::velox::exec
