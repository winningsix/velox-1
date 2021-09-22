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
#include "string.h"
#include "velox/cider/core/CiderRowExpr.h"
#include "nlohmann/json.hpp"
#include "nlohmann/fifo_map.hpp"
using json = nlohmann::json;
template<class K, class V, class dummy_compare, class A>
using tmp_fifo_map = nlohmann::fifo_map<K, V, nlohmann::fifo_map_compare<K>, A>;
using tmp_json = nlohmann::basic_json<tmp_fifo_map>;

namespace intel::cider::core{
 typedef std::string PlanNodeId;
 class CiderPlanNode {
  public:
   explicit CiderPlanNode(const PlanNodeId& id) : id_{id} {}

   virtual ~CiderPlanNode() {}

   const PlanNodeId& id() const {
     return id_;
   }

   virtual const std::vector<std::shared_ptr<const CiderPlanNode>>& sources() const = 0;

   virtual json toCiderJSON(std::string ciderPlanId) const = 0;

   virtual std::string name() const = 0;

   private:
    const PlanNodeId id_;
 };

 class CiderProjectNode : public CiderPlanNode {
  public:
   CiderProjectNode(
       const PlanNodeId& id,
       std::vector<std::string> assignmentKeys,
       std::vector<std::shared_ptr<const CiderRowExpr>> assignmentExprs,
       std::shared_ptr<const CiderPlanNode> source )
       : CiderPlanNode(id), assignmentKeys_{assignmentKeys}, 
       assignmentExprs_(assignmentExprs), sources_{source} {}

   json toCiderJSON(std::string ciderPlanId) const override {
    json projectNode = json::object();
    projectNode.push_back({"id", ciderPlanId});
    projectNode.push_back({"relOp", name()});
    tmp_json fieldNodes = tmp_json::array();
    tmp_json fieldExprs = tmp_json::array();
    for (int i=0; i < assignmentKeys_.size(); i++) {
     fieldNodes.push_back({assignmentKeys_.at(i)});
     fieldExprs.push_back({assignmentExprs_.at(i)->toCiderJSON()});
    }
    projectNode.push_back({"fields", fieldNodes});
    projectNode.push_back({"exprs", fieldExprs});

    return projectNode;
   }

   std::string name() const override {
    return "LogicalProject";
   }

   const std::vector<std::shared_ptr<const CiderPlanNode>>& sources() const override {
    return sources_;
   }

  private:
   const std::vector<std::string> assignmentKeys_;
   const std::vector<std::shared_ptr<const CiderRowExpr>> assignmentExprs_;
   const std::vector<std::shared_ptr<const CiderPlanNode>> sources_;   
 };

 class CiderFilterNode : public CiderPlanNode {
  public:
   CiderFilterNode(
       const PlanNodeId& id,
       std::shared_ptr<const CiderRowExpr> filter,
       std::shared_ptr<const CiderPlanNode> source)
       : CiderPlanNode(id), filter_(filter), sources_{source} {}

   json toCiderJSON(std::string ciderPlanId) const override {
    json filterNode = json::object();
    filterNode.push_back({"id", ciderPlanId});
    filterNode.push_back({"relOp", name()});
    filterNode.push_back({"condition", filter_->toCiderJSON()});
    return filterNode;
   }

   std::string name() const override {
    return "LogicalFilter";
   }

   const std::vector<std::shared_ptr<const CiderPlanNode>>& sources() const override {
    return sources_;
   }

  private:
   const std::shared_ptr<const CiderRowExpr> filter_;
   const std::vector<std::shared_ptr<const CiderPlanNode>> sources_;
 };

 class CiderTableScanNode : public CiderPlanNode {
  public:
   CiderTableScanNode(
       const PlanNodeId& id,
       std::string tableName,
       std::string schemaName,
       std::vector<std::string> fieldNames)
       : CiderPlanNode(id), tableName_(tableName),
       schemaName_(schemaName), fieldNames_(fieldNames) {}; 

   std::string name() const override {
    return "LogicalTableScan";
   }

   json toCiderJSON(std::string ciderPlanId) const override {
    json scanNode = json::object();
    scanNode.push_back({"id", ciderPlanId});
    scanNode.push_back({"name", name()});

    tmp_json fieldJsonNames = tmp_json::array();
    for (std::string fieldName : fieldNames_) {
     fieldJsonNames.push_back(fieldName);
    }
    scanNode.push_back({"fieldNames", fieldJsonNames});
    json tableInfoNode = json::object();
    tableInfoNode.push_back({schemaName_, tableName_});
    scanNode.push_back({"table", tableInfoNode});
    json inputNode = json::array();
    scanNode.push_back({"input", inputNode});
    return scanNode;
   }

   const std::vector<std::shared_ptr<const CiderPlanNode>>& sources() const override;
  private:
   const std::string tableName_;
   const std::string schemaName_;
   const std::vector<std::string> fieldNames_;
 };
}
