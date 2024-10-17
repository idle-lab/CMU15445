//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

void SortTupleHelperFunction(std::vector<Tuple> &tuples, const std::vector<AbstractExpressionRef> &order_bys,
                             const Schema &tuple_schema) {
  std::sort(result_.begin(), result_.end(), [&order_bys, &tuple_schema](const Tuple &left, const Tuple &right) -> bool {
    for (auto &x : order_bys) {
      auto left_value = x.second->Evaluate(&left, tuple_schema);
      auto rigth_value = x.second->Evaluate(&right, tuple_schema);
      if (left_value.CompareEquals(rigth_value) == CmpBool::CmpTrue) {
        continue;
      }
      if (((x.first == OrderByType::DEFAULT || x.first == OrderByType::ASC) &&
           left_value.CompareLessThan(rigth_value) == CmpBool::CmpTrue) ||
          (x.first == OrderByType::DESC && left_value.CompareGreaterThan(rigth_value) == CmpBool::CmpTrue)) {
        return true;
      }
      break;
    }
    return false;
  });
}

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> result_;
  size_t pos_;
};
}  // namespace bustub
