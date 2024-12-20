//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // this func only execute once
  if (is_over_) {
    return false;
  }
  is_over_ = true;
  Tuple tp;
  int32_t cnt = 0;
  auto* txn = exec_ctx_->GetTransaction();
  auto temp_ts = txn->GetTransactionTempTs();
  while (child_executor_->Next(&tp, rid)) {
    // insert data
    auto rid_opt = table_info_->table_->InsertTuple({temp_ts, false}, tp);
    if (!rid_opt.has_value()) {
      continue;
    }
    txn->AppendWriteSet(table_info_->oid_, rid_opt.value());

    // update indexs
    for (auto &index_info : indexs_info_) {
      auto key =
          tp.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, rid_opt.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{Column{"num_of_inserted", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = Tuple{values, &sch};
  return true;
}

}  // namespace bustub
