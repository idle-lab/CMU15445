#include "execution/executors/window_function_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  // Initialization
  child_executor_->Init();
  result_.clear();
  it_ = 0;

  // Get all tuple.
  std::unordered_map<RID, std::vector<Value>> tmp_result;
  std::vector<Tuple> tuples;
  Tuple tuple;
  RID rid;
  int cnt = 0;
  while (child_executor_->Next(&tuple, &rid)) {
    // Make a identifier for every tuple.
    rid.Set(cnt++, 0);
    tuple.SetRid(rid);
    tuples.emplace_back(std::move(tuple));
    // Allocate space in advance
    tmp_result[rid].resize(plan_->columns_.size());
  }

  // Caculate result.
  auto &tuple_schema = child_executor_->GetOutputSchema();
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by;
  for (size_t i = 0; i < plan_->columns_.size(); ++i) {
    auto wf_ptr = plan_->window_functions_.find(i);
    if (wf_ptr == plan_->window_functions_.end()) {
      // No window function, just evaluate the tuple value.
      for (auto &tuple : tuples) {
        tmp_result[tuple.GetRid()][i] = plan_->columns_[i]->Evaluate(&tuple, tuple_schema);
      }
      continue;
    }
    // Have a window function, calculate aggregation result.
    // Sort tuple by keys(partition_by_, order_exprs).
    const auto &wf = wf_ptr->second;
    auto wf_helper = std::make_unique<WindowFunctionHelper>(wf.type_);
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_exprs;
    for (auto &expr : wf.partition_by_) {
      order_exprs.emplace_back(OrderByType::ASC, expr);
    }
    for (auto &expr : wf.order_by_) {
      order_exprs.emplace_back(expr);
    }
    SortTupleHelperFunction(tuples, order_exprs, tuple_schema);

    // Aggregation the result value of each tuple.
    std::vector<Value> partion_keys;
    std::vector<Value> now_keys;
    if (wf.order_by_.empty()) {
      // When order by and window frames are both omitted,
      // it calculates from the first row to the last row for each partition,
      // which means the results within the partition should be the same.
      std::vector<RID> rids_stack;
      for (auto &tuple : tuples) {
        for (auto &expr : wf.partition_by_) {
          now_keys.emplace_back(expr->Evaluate(&tuple, tuple_schema));
        }
        if (!wf.partition_by_.empty() && (partion_keys.empty() || !IsEqual(partion_keys, now_keys))) {
          // Store the result of the whole partion.
          while (!rids_stack.empty()) {
            tmp_result[rids_stack.back()][i] = wf_helper->Get();
            rids_stack.pop_back();
          }
          partion_keys = std::move(now_keys);
          wf_helper->Init();
        }
        wf_helper->Combine(wf.function_->Evaluate(&tuple, tuple_schema));
        rids_stack.emplace_back(tuple.GetRid());
        now_keys.clear();
      }
      // Store the last partion result.
      while (!rids_stack.empty()) {
        tmp_result[rids_stack.back()][i] = wf_helper->Get();
        rids_stack.pop_back();
      }
      continue;
    }

    std::vector<Value> now_rank_keys;
    std::vector<Value> rank_keys;
    Value same_num = ValueFactory::GetIntegerValue(1);
    // When window frames are omitted and order by clauses not omitted,
    // it calculates from the first row to the current row for each partition.
    for (auto &tuple : tuples) {
      for (auto &expr : wf.partition_by_) {
        now_keys.emplace_back(expr->Evaluate(&tuple, tuple_schema));
      }
      if (wf.type_ == WindowFunctionType::Rank) {
        for (auto &expr : wf.order_by_) {
          now_rank_keys.emplace_back(expr.second->Evaluate(&tuple, tuple_schema));
        }
      }
      if (!wf.partition_by_.empty() && (partion_keys.empty() || !IsEqual(partion_keys, now_keys))) {
        partion_keys = std::move(now_keys);
        rank_keys = now_rank_keys;
        wf_helper->Init();
      }
      if (wf.type_ == WindowFunctionType::Rank) {
        if (rank_keys.empty() || !IsEqual(rank_keys, now_rank_keys)) {
          wf_helper->Combine(same_num);
          rank_keys = now_rank_keys;
          same_num = ValueFactory::GetIntegerValue(1);
        } else {
          same_num = same_num.Add(ValueFactory::GetIntegerValue(1));
        }
      } else {
        wf_helper->Combine(wf.function_->Evaluate(&tuple, tuple_schema));
      }
      tmp_result[tuple.GetRid()][i] = wf_helper->Get();
      now_keys.clear();
      now_rank_keys.clear();
    }
  }

  for (auto &values : tmp_result) {
    result_.emplace_back(std::move(values.second), &plan_->OutputSchema());
  }
  if (!plan_->window_functions_.empty()) {
    SortTupleHelperFunction(result_, plan_->window_functions_.begin()->second.order_by_, plan_->OutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ >= result_.size()) {
    return false;
  }
  *tuple = std::move(result_[it_++]);
  return true;
}

}  // namespace bustub
