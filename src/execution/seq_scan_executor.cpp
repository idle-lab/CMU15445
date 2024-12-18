//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  rids_.clear();
  rid_it_ = 0;
  // Get all rid.
  for (auto tit = table_info_->table_->MakeIterator(); !tit.IsEnd(); ++tit) {
    rids_.push_back(tit.GetRID());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::optional<Tuple> res;
  while (rid_it_ < rids_.size()) {
    auto tp = table_info_->table_->GetTuple(rids_[rid_it_]);
    // 1. The tuple in the table heap is the most recent data. 
    //    In this case, the sequential scan executor may directly return the tuple, 
    //    or skip the tuple if it has been removed.
    if (tp.first.ts_ == exec_ctx_->GetTransaction()->GetReadTs()) {
      if (tp.first.is_deleted_) {
        continue;
      }
      res.emplace(tp.second);
      rid_it_++;
      break;
    }

    // 2. The tuple in the table heap contains modification by the current transaction.
    //    The current transaction has a “transaction temporary timestamp”, 
    //    which is computed by `TXN_START_ID + txn_human_readable_id = txn_id`.
    //    Executor should returns the tuple that modified by current transaction to user directly.
    // 
    // 3. The tuple in the table heap is
    //    (1) modified by another uncommitted transaction, 
    //    (2) newer than the transaction read timestamp. 
    //    In this case, you will need to iterate the version chain to collect all undo logs 
    //    after the read timestamp, and recover the past version of the tuple.
    auto undo_logs = CollectUndoLogs(exec_ctx_->GetTransactionManager(), tp.second.GetRid(),
                                     exec_ctx_->GetTransaction()->GetReadTs(), 
                                     exec_ctx_->GetTransaction()->GetTransactionTempTs());
    if (undo_logs.has_value()) {
      res = ReconstructTuple(&table_info_->schema_, tp.second, tp.first, undo_logs.value());

      // Executor should emit the tuples that have not been deleted and satisfy predicates
      if (res.has_value() && (plan_->filter_predicate_ == nullptr ||
                                    plan_->filter_predicate_->Evaluate(&res.value(), table_info_->schema_).GetAs<bool>())) {
        break;
      }
    }

    rid_it_++;
  }
  if (rid_it_ >= rids_.size()) {
    return false;
  }
  *tuple = std::move(res.value());
  *rid = rids_[rid_it_++];
  return true;
}

}  // namespace bustub
