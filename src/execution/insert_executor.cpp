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

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // this func only execute once
  if (is_over_) {
    return false;
  }
  is_over_ = true;
  Tuple tp;
  RID rd;
  int32_t cnt = 0;
  while (child_executor_->Next(&tp, &rd)) {
    // insert data
    auto rd = table_info_->table_->InsertTuple({INVALID_TXN_ID, false}, tp);
    if (rd == std::nullopt) {
      continue;
    }
    // update indexs
    for (auto &index_info : indexs_info_) {
      auto key =
          tp.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, rd.value(), exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{Column{"__num_of_inserted", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = Tuple{values, &sch};
  return true;
}

}  // namespace bustub
