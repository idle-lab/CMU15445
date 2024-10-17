//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(left_executor)),
      right_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_over_ = false;
  cnt = 0;
  if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
    is_over_ = true;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (is_over_) {
      return false;
    }
    for (; right_executor_->Next(&right_tuple, &right_rid);) {
      if (plan_->predicate_
              ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                             right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &plan_->OutputSchema()};
        cnt++;
        return true;
      }
    }

    // When the join type is LEFT join and right table have no tuple match with current left tuple,
    // You should emit the tuple join with right tuple that all values are integer_null.
    if (plan_->join_type_ == JoinType::LEFT && cnt == 0) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      *tuple = Tuple{values, &plan_->OutputSchema()};
      right_executor_->Init();
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        is_over_ = true;
      }
      return true;
    }
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      is_over_ = true;
    }
    right_executor_->Init();
    cnt = 0;
  }
}

}  // namespace bustub
