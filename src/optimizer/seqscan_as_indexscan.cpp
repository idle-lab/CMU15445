#include "include/execution/expressions/column_value_expression.h"
#include "include/execution/expressions/comparison_expression.h"
#include "include/execution/plans/filter_plan.h"
#include "include/execution/plans/index_scan_plan.h"
#include "include/execution/plans/seq_scan_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->children_) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // must be a SeqScan Executor
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  auto *table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
  const auto &indexs_info = catalog_.GetTableIndexes(table_info->name_);
  auto expr = std::dynamic_pointer_cast<ComparisonExpression>(seq_scan_plan.filter_predicate_);
  // pred must be Equal
  if (expr == nullptr || expr->comp_type_ != ComparisonType::Equal || indexs_info.empty()) {
    return optimized_plan;
  }

  for (auto &child : expr->children_) {
    // column may in left or right
    if (auto col = std::dynamic_pointer_cast<ColumnValueExpression>(child); col != nullptr) {
      std::swap(child, *expr->children_.begin());
      // find if have a index in this column
      for (const auto &index_info : indexs_info) {
        auto &attrs = index_info->index_->GetKeyAttrs();
        // Index column index must match predicate column
        if (attrs.size() == 1 && attrs[0] == col->GetColIdx()) {
          auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(expr->children_[1]);
          if (pred_key == nullptr) {
            return optimized_plan;
          }
          return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                     index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                                     pred_key.get());
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
