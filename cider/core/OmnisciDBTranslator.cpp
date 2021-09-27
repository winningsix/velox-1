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
#pragma once
#include "OmnisciDBTranslator.h"
#include <stack>
#include <set>

namespace intel::cider::core {
std::string OmnisciDBTranslator::toRelAlgStr(
    const std::shared_ptr<const ComputeIRNode>& node) {
  // return json string
  std::stack<std::shared_ptr<const ComputeIRNode>> planNodes;
  std::set<std::shared_ptr<const ComputeIRNode>> visited;
  planNodes.emplace(node);
  visited.emplace(node);
  std::shared_ptr<const ComputeIRNode> currentNode;
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
    relsNode.push_back(
        currentNode->toOmnisciDBJSON(std::to_string(ciderPlanId)));
    ciderPlanId++;
    planNodes.pop();
  }
  rootNode.push_back({"relsNode", relsNode});
  return rootNode.dump();
}
} // namespace intel::cider::core
