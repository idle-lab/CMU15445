//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_over_) {
    return false;
  }
  is_over_ = true;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple tp;
  RID rd;
  int32_t cnt = 0;
  while (child_executor_->Next(&tp, &rd)) {
    table_info->table_->UpdateTupleMeta({INVALID_TXN_ID, true}, rd);
    for (auto &index_info : indexs_info) {
      auto key =
          tp.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, rd, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{{"__num_of_deleted", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = {values, &sch};
  return true;
}

}  // namespace bustub
