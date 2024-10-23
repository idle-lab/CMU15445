#include "execution/executors/limit_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/topn_executor.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(Bernie): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(children);

  if (optimized_plan->GetType() != PlanType::Limit ||
      !(children.size() == 1 && optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort)) {
    return optimized_plan;
  }

  const auto *limit_plan = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
  auto sort_plan = std::dynamic_pointer_cast<const SortPlanNode>(optimized_plan->GetChildAt(0));
  return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, sort_plan->GetChildAt(0), sort_plan->order_bys_,
                                        limit_plan->GetLimit());
}

}  // namespace bustub
