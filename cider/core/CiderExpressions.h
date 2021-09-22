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
#include "velox/cider/core/CiderRowExpr.h"

using json = nlohmann::json;

namespace intel::cider::core{
 namespace {
  std::string matchType(std::string type) {
   if (type == "int" || type == "integer" || type == "Int") {
    return "Integer";
   }
   if (type == "double") {
    return "decimal";
   }
   return type;
  }

  int getTypePrecision(std::string type) {
   if (type == "int" || type == "integer" || type == "Int") {
    return 10;
   }
   if (type == "double") {
    return 15;
   }
   return -1;
   }

  std::string matchOperator(std::string originOp) {
   if(originOp == "EQUAL") {
    return "=";
   }
   if(originOp == "GREATER_THAN") {
    return ">=";
   }
   if(originOp == "LESS_THAN") {
    return "<";
   }
   if(originOp == "MODULUS") {
    return "MOD";
   }
   if(originOp == "DIVIDE") {
    return "/";
   }
   return originOp;
   }
 }

 class CiderConstantExpr : public CiderRowExpr {
  public:
   CiderConstantExpr(std::string value, std::string type)
   : type_{move(type)}, value_{move(value)} {}

   std::string value() const{
    return value_;
   }

   std::string type() const{
    return type_;
   }

   json toCiderJSON() const override {
    json j = json::object();
    if (type_ == "varchar") {
     j.push_back({"literal", value_});
     j.push_back({"type", "CHAR"});
     j.push_back({"target_type", "CHAR"});
     j.push_back({"scale", -2147483648});
     j.push_back({"precision", value_.length()});
     j.push_back({"type_scale", -2147483648});
     j.push_back({"type_precision", value_.length()});
    }
    if (type_ == "bigint" || type_ == "integer") {
     j.push_back({"literal", std::stoi(value_)});
     j.push_back({"type", "DECIMAL"});
     j.push_back({"target_type", matchType(type_)});
     j.push_back({"scale", 0});
     j.push_back({"precision", value_.length()});
     j.push_back({"type_scale", 0});
     j.push_back({"type_precision", getTypePrecision(type_)});
    }
    if (type_ == "double") {
     int precision = value_.length() - 1;
     int scale = precision - value_.find(".");
     int pos = value_.find(".");
     std::string val = value_;
     val = val.erase(pos, 1);
     j.push_back({"literal", std::stoi(val)});
     j.push_back({"type", "DECIMAL"});
     j.push_back({"target_type", matchType(type_)});
     j.push_back({"scale", scale});
     j.push_back({"precision", precision});
     j.push_back({"type_scale", scale});
     j.push_back({"type_precision", precision});
    }
    if (type_ == "bool") {
     j.push_back({"literal", value_});
     j.push_back({"type", "BOOLEAN"});
     j.push_back({"target_type", "BOOLEAN"});
     j.push_back({"scale", -2147483648});
     j.push_back({"precision", 1});
     j.push_back({"type_scale", -2147483648});
     j.push_back({"type_precision", 1});
   }

    return j;
   }

  private:
   const std::string type_;
   const std::string value_;
 };

 class CiderVariableExpr : public CiderRowExpr {
  public:
  CiderVariableExpr(std::string name, std::string type, int index)
  : name_{move(name)}, type_{move(type)}, index_(index) {}

  json toCiderJSON() const override {
   json j = json::object();
   j.push_back({"input", std::to_string(index_)});
   return j;
  }
  
  private:
   const std::string name_;
   const std::string type_;
   const int index_;
 };

// row expression that describes a predicate, such as "field_a > 2"
class CiderCallExpr : public CiderRowExpr {
 public:
 CiderCallExpr(std::string op, std::string type, std::vector<std::shared_ptr<const CiderRowExpr>>& expressions)
 : op_{move(op)}, type_{move(type)}, expressions_{move(expressions)} {}

 json toCiderJSON() const override {
  json opNode = json::object();
  opNode.push_back({"op", matchOperator(op_)});
  json exprNodes = json::array();
  for (auto expr : expressions_) {
   json exprNode = expr->toCiderJSON();
   exprNodes.push_back(exprNode);
  }
  opNode.push_back({"operands", exprNodes});
  json typeNode = json::object();
  std::string typeForChange = type_;
  std::transform(typeForChange.begin(), typeForChange.end(), typeForChange.begin(), ::toupper);
  typeNode.push_back({"type",typeForChange});
  typeNode.push_back({"nullable", true});
  opNode.push_back({"type", typeNode});
  return opNode;
 }

 private:
  const std::string op_;
  const std::string type_;
  const std::vector<std::shared_ptr<const CiderRowExpr>> expressions_;
};
}
