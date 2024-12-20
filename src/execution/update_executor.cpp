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
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

/**
 * @todo 1. 考虑并发 update，为 IsWriteWriteConflict 添加 check，使用 in_progress_
 *       2. 被自己重复更新
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_over_) {
    return false;
  }
  is_over_ = true;
  Tuple tp;
  RID rd;
  int32_t cnt = 0;
  auto* txn = exec_ctx_->GetTransaction();
  auto* txn_mgr = exec_ctx_->GetTransactionManager();
  std::vector<std::tuple<RID, TupleMeta, Tuple>> old_data;

  while (child_executor_->Next(&tp, &rd)) {
    auto [meta, tuple] = table_info_->table_->GetTuple(rd);
    if (IsWriteWriteConflict(txn, meta.ts_)) {
      txn->SetTainted();
      throw ExecutionException("update operation has a write-write conflict.");
    }
    old_data.emplace_back(std::move(rd), std::move(meta), std::move(tuple));
  }

  for(auto& [rid, old_meta, old_tuple] : old_data) {
    // Generate the updated tuple, for updates.
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));
    }
    Tuple new_tuple{values, &table_info_->schema_};

    auto version_link = txn_mgr->GetVersionLink(rid);

    if (old_meta.ts_ == txn->GetTransactionTempTs()) {
      if (version_link.has_value()) {
        // Consider the following situation:
        // RID=0/0 ts=txn2 tuple=(1, 2, 1) 
        //         txn2_0 (_, 1, _) ts=3
        // We need update version chain.

        // Rebuilding unmodified tuples.
        auto pre_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
        auto pre_schema = GetUndoLogSchema(&table_info_->schema_, pre_undo_log);
        size_t col_num = table_info_->schema_.GetColumnCount();
        std::vector<bool> modified_fields(col_num);
        std::vector<Value> values;
        std::vector<Column> cols;

        for (size_t i = 0, j = 0;i < col_num;++i) {
          if (pre_undo_log.modified_fields_[i]) {
            modified_fields[i] = true;
            values.emplace_back(pre_undo_log.tuple_.GetValue(&pre_schema, j));
            cols.emplace_back(pre_schema.GetColumn(j++));
            continue;
          }
          if (new_tuple.GetValue(&table_info_->schema_, i).CompareExactlyEquals(old_tuple.GetValue(&table_info_->schema_, i))) {
            continue;
          }
          modified_fields[i] = true;
          values.emplace_back(old_tuple.GetValue(&table_info_->schema_, i));
          cols.emplace_back(table_info_->schema_.GetColumn(i));
        }

        // Update undo log in place.
        Schema schema{cols};
        txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, UndoLog{false, modified_fields, Tuple{values, &schema}, pre_undo_log.ts_, pre_undo_log.prev_version_});
      }
      // If the situation is:
      // RID=0/0 ts=txn2 tuple=(1, 2, 1)
      // We just update tuple in place, not update version chain.
    } else {
      // Create the undo log for the modification. 
      auto undo_log = GenerateDiffLog(&table_info_->schema_, &new_tuple, &old_tuple);
      undo_log.ts_ = old_meta.ts_;
      undo_log.is_deleted_ = old_meta.is_deleted_;

      if (version_link.has_value()) {
        undo_log.prev_version_ = version_link->prev_;
      }

      // Update the next version link of the tuple to point to the new undo log.
      auto undo_link = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateVersionLink(rid, std::make_optional<VersionUndoLink>({undo_link, true}));
    }

    // Update the base tuple and base meta in the table heap.
    table_info_->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, rid);
    txn->AppendWriteSet(table_info_->oid_, rid);

    // update indexs
    for (auto &index_info : indexs_info_) {
      // delete old index
      auto key =
          tp.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, rd, exec_ctx_->GetTransaction());

      // insert new index
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                         index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{Column{"num_of_updated", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = Tuple{values, &sch};
  return true;
}

}  // namespace bustub
