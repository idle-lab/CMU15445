#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      heap_(plan) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  heap_.Clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    if (heap_.Size() < plan_->n_ || heap_.Compare(tuple, heap_.Top())) {
      heap_.Push(std::move(tuple));
      if (heap_.Size() > plan_->n_) {
        heap_.Pop();
      }
    }
  }
  while (!heap_.Empty()) {
    result_.emplace_front(heap_.Pop());
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }
  *tuple = std::move(result_.front());
  result_.pop_front();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.Size(); };

}  // namespace bustub
