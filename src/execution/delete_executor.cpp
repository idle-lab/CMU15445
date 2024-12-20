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

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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


    auto version_link = txn_mgr->GetVersionLink(rid);

    if (old_meta.ts_ == txn->GetTransactionTempTs()) {
      // If the current transaction already modified the tuple and has an undo log for the tuple,
      // it should update the existing undo log instead of creating new ones.
      if (version_link.has_value()) {
        
        // Rebuilding unmodified tuples.
        auto pre_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
        old_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, old_meta, {pre_undo_log}).value();

        // Recreate the undo log for the modification. 
        auto undo_log = GenerateDiffLog(&table_info_->schema_, nullptr, &old_tuple);
        undo_log.ts_ = pre_undo_log.ts_;
        undo_log.prev_version_ = pre_undo_log.prev_version_;

        undo_log.prev_version_ = txn_mgr->GetUndoLog(version_link->prev_).prev_version_;
        txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, undo_log);
      }
      // If the situation is:
      // RID=0/0 ts=txn2 tuple=(1, 2, 1)
      // We just update tuple in place, not update version chain.
    } else {
      // Create the undo log for the modification.
      auto undo_log = GenerateDiffLog(&table_info_->schema_, nullptr, &old_tuple);
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
    table_info_->table_->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), true}, old_tuple, rid);
    txn->AppendWriteSet(table_info_->oid_, rid);

    // delete indexs
    for (auto &index_info : indexs_info_) {
      auto key =
          old_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }
  Schema sch{{{"__num_of_deleted", TypeId::INTEGER}}};
  std::vector<Value> values{{TypeId::INTEGER, cnt}};
  *tuple = {values, &sch};
  return true;
}

}  // namespace bustub
