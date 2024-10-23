#include "execution/executors/sort_executor.h"

namespace bustub {

void SortTupleHelperFunction(std::vector<Tuple> &tuples,
                             const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                             const Schema &tuple_schema) {
  std::sort(tuples.begin(), tuples.end(), [&order_bys, &tuple_schema](const Tuple &left, const Tuple &right) -> bool {
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

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  pos_ = 0;

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    result_.emplace_back(std::move(tuple));
  }
  SortTupleHelperFunction(result_, plan_->order_bys_, plan_->OutputSchema());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (pos_ >= result_.size()) {
    return false;
  }
  *tuple = std::move(result_[pos_++]);
  return true;
}

}  // namespace bustub
