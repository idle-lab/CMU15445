//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::forward<std::unique_ptr<AbstractExecutor>>(left_child)),
      right_child_(std::forward<std::unique_ptr<AbstractExecutor>>(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  ht_.clear();
  is_over_ = false;
  now_value = nullptr;

  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    ht_[MakeLeftHashJoinKey(&tuple)].tuples_.emplace_back(std::move(tuple));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_over_) {
    return false;
  }
  // find a vaild right tuple.
  while (!is_right_over_ && (now_value == nullptr || value_it_ >= now_value->tuples_.size())) {
    if (!right_child_->Next(&right_tuple_, &right_rid_)) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        now_value = nullptr;
        is_right_over_ = true;
        break;
      }
      is_over_ = true;
      return false;
    }
    auto right_key = MakeRightHashJoinKey(&right_tuple_);
    if (ht_.find(right_key) != ht_.end()) {
      now_value = &ht_[right_key];
      ht_[right_key].matched_ = true;
      value_it_ = 0;
      break;
    }
  }

  // Find the left tuple that no right tuple matched with it and emit it.
  if (is_right_over_) {
    while (!ht_.empty()) {
      if (ht_.begin()->second.matched_ || ht_.begin()->second.tuples_.empty()) {
        ht_.erase(ht_.begin());
        continue;
      }
      std::vector<Value> values;
      for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ht_.begin()->second.tuples_.back().GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      ht_.begin()->second.tuples_.pop_back();
      return true;
    }
    is_over_ = true;
    return false;
  }

  // join left and right tuples.
  std::vector<Value> values;
  for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
    values.emplace_back(now_value->tuples_[value_it_].GetValue(&left_child_->GetOutputSchema(), i));
  }
  for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
    values.emplace_back(right_tuple_.GetValue(&right_child_->GetOutputSchema(), i));
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  value_it_++;
  return true;
}

}  // namespace bustub
