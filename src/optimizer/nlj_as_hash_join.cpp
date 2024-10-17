#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ParseLogicExpression(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> *left_key_expressions,
                          std::vector<AbstractExpressionRef> *right_key_expressions) -> bool {
  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(expr);
  if (logic_expr != nullptr) {
    return ParseLogicExpression(expr->GetChildAt(0), left_key_expressions, right_key_expressions) &&
           ParseLogicExpression(expr->GetChildAt(1), left_key_expressions, right_key_expressions);
  }
  auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr);
  if (comparison_expr == nullptr || comparison_expr->comp_type_ != ComparisonType::Equal) {
    // ComparisonExpression Must be equi-condition.
    return false;
  }
  auto left_child = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(0));
  auto right_child = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(1));
  if (left_child == nullptr || right_child == nullptr) {
    return false;
  }
  // It is possible that the column from outer table is on the right side of the equi-condition.
  if (left_child->GetTupleIdx() == 1) {
    std::swap(left_child, right_child);
  }
  left_key_expressions->push_back(left_child);
  right_key_expressions->push_back(right_child);
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(children);

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  auto *njl_plan = dynamic_cast<NestedLoopJoinPlanNode *>(optimized_plan.get());
  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;
  if (!ParseLogicExpression(njl_plan->Predicate(), &left_key_expressions, &right_key_expressions)) {
    return optimized_plan;
  }
  return std::make_shared<HashJoinPlanNode>(njl_plan->output_schema_, njl_plan->GetLeftPlan(), njl_plan->GetRightPlan(),
                                            left_key_expressions, right_key_expressions, njl_plan->GetJoinType());
}

}  // namespace bustub
