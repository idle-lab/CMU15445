#include "execution/executors/sort_executor.h"

namespace bustub {

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
