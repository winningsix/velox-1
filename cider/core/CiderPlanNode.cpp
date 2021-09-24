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
#include <stack>
#include <set>
#include "velox/cider/core/CiderPlanNode.h"

namespace intel::cider::core {
namespace {
static const std::vector<std::shared_ptr<const CiderPlanNode>> EMPTY_SOURCES;
}

const std::vector<std::shared_ptr<const CiderPlanNode>>&
CiderTableScanNode::sources() const {
  return EMPTY_SOURCES;
}

std::string CiderPlanNode::toCiderRelAlgStr(
    const std::shared_ptr<CiderPlanNode>& node) {
  // return json string
  std::stack<std::shared_ptr<const CiderPlanNode>> planNodes;
  std::set<std::shared_ptr<const CiderPlanNode>> visited;
  planNodes.emplace(node);
  visited.emplace(node);
  std::shared_ptr<const CiderPlanNode> currentNode;
  std::string lastConvertedPlanId;
  json rootNode = json::object();
  json relsNode = json::array();
  int ciderPlanId = 0;
  while (!planNodes.empty()) {
    currentNode = planNodes.top();
    lastConvertedPlanId = currentNode->id();
    if (currentNode->sources().size() > 0 &&
        currentNode->sources().at(0) != nullptr) {
      bool isAllVisited = true;
      for (auto source : currentNode->sources()) {
        if (visited.count(source) == 0) {
          visited.emplace(source);
          planNodes.emplace(source);
          isAllVisited = false;
          currentNode = source;
          lastConvertedPlanId = source->id();
          break;
        }
      }
      if (!isAllVisited) {
        continue;
      }
    }
    // add json
    relsNode.push_back(currentNode->toCiderJSON(std::to_string(ciderPlanId)));
    ciderPlanId++;
    planNodes.pop();
  }
  rootNode.push_back({"relsNode", relsNode});
  return rootNode.dump();
}

} // namespace intel::cider::core
