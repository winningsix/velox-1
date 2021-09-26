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

#ifdef CIDER_WITH_OMNISCI
#include <Embedded/DBEngine.h>
#endif

namespace facebook::velox::exec {

class CiderQueryRunner {
 public:
  static CiderQueryRunner* getInstance();

  void createTable(){};
  void processBlocks(){};

  std::string getBackend() {
    return backend_;
  }

  //  void createTable(
  //      const std::string& name,
  //      const struct ArrowArray* tableData,
  //      const struct ArrowSchema* tableSchema);
  //
  //  struct ArrowArray* processBlocks(
  //      const std::string& sql,
  //      const struct ArrowArray* inputData,
  //      const struct ArrowSchema* inputSchema,
  //      const struct ArrowSchema* outputSchema);

  void cleanup() {
#ifdef CIDER_WITH_OMNISCI
    if (dbe_) {
      dbe_.reset();
    }
#endif
  };

 private:
  void init();
#ifdef CIDER_WITH_OMNISCI
  void initOminisciDBEngine();
  std::shared_ptr<EmbeddedDatabase::DBEngine> dbe_;
#endif
  static std::unique_ptr<CiderQueryRunner> instance_;
  static std::string backend_;
};

} // namespace facebook::velox::exec
