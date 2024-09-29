//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void UpdateExecutor::Init() {
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_over_) {
    return false;
  }
  is_over_ = true;
  Tuple tp;
  RID rd;
  int32_t cnt = 0;
  while (child_executor_->Next(&tp, &rd)) {
    // delete
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, true}, rd);
    for (auto &index_info : indexs_info_) {
      auto key =
          tp.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(tp, rd, exec_ctx_->GetTransaction());
    }

    // insert
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&tp, table_info_->schema_));
    }
    Tuple new_tp{values, &table_info_->schema_};
    auto new_rd = table_info_->table_->InsertTuple({INVALID_TXN_ID, false}, new_tp);
    if (new_rd == std::nullopt) {
      continue;
    }
    for (auto &index_info : indexs_info_) {
      auto key = new_tp.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                     index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_tp, new_rd.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{Column{"__num_of_updated", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = Tuple{values, &sch};
  return true;
}

}  // namespace bustub
