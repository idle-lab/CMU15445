//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->aggregates_, plan_->agg_types_);
  Tuple tp;
  RID rd;
  aht_->Clear();
  // You shold Get all tuples and compute the aggregation result in AggregationExecutor::Init()
  while (child_executor_->Next(&tp, &rd)) {
    auto key = MakeAggregateKey(&tp);
    auto value = MakeAggregateValue(&tp);
    aht_->InsertCombine(key, value);
    is_empyt_ = false;
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_empyt_) {
    // When there is no tuple in table and you use 'group by', there should be no output.
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    *tuple = Tuple{aht_->GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
    is_empyt_ = false;
    return true;
  }
  if (*aht_iterator_ == aht_->End()) {
    return false;
  }
  std::vector<Value> values(std::move(aht_iterator_->Key().group_bys_));
  for (auto &value : aht_iterator_->Val().aggregates_) {
    values.emplace_back(std::move(value));
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  ++(*aht_iterator_);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
