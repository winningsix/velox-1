/*
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

#include <atomic>
#include <iostream>
#include <mutex>
#include <random>

#include <boost/filesystem.hpp>

#include "velox/exec/HybridExecutor.h"

namespace facebook::velox::exec {

#ifdef EXEC_WITH_OMNISCI
void HybridExecutor::initOminisciDBEngine() {
  std::string base_path =
      boost::filesystem::unique_path(
          boost::filesystem::temp_directory_path() / "omnidbe-%%%%-%%%%-%%%%")
          .string();
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(50000, 60000);
  int calcite_port = distrib(gen);
  std::string opt_str =
      base_path + " --calcite-port " + std::to_string(calcite_port);
  dbe_ = EmbeddedDatabase::DBEngine::create(opt_str);
}
#endif

void HybridExecutor::init() {
  std::transform(
      backend_.begin(), backend_.end(), backend_.begin(), [](unsigned char c) {
        return std::tolower(c);
      });
  if (backend_ == "omnisci") {
#ifdef EXEC_WITH_OMNISCI
    initOminisciDBEngine();
#else
    backend_ = "default";
    std::cerr
        << "omnisci backend support not built, fallback to default backend"
        << std::endl;
#endif
  } else {
    std::cerr << "Unsupported backend \'" << backend_
              << "\', fallback to default backend" << std::endl;
    backend_ = "default";
  }
  return;
}

std::unique_ptr<HybridExecutor> HybridExecutor::instance_ = nullptr;
std::string HybridExecutor::backend_ = "default";

HybridExecutor* HybridExecutor::getInstance() {
  static std::mutex mutex;
  static std::atomic<bool> cider_query_runner_inited(false);

  if (!cider_query_runner_inited) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!cider_query_runner_inited) {
      backend_ = getenv("driver.hybrid.execution.backends");
      instance_.reset(new HybridExecutor());
      instance_->init();
      cider_query_runner_inited = true;
    }
  }
  return instance_.get();
}

} // namespace facebook::velox::exec
