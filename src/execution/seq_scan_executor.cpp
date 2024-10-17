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
  std::pair<TupleMeta, Tuple> tp;
  while (rid_it_ < rids_.size()) {
    tp = table_info_->table_->GetTuple(rids_[rid_it_]);
    // Executor should emit the tuples that have not been deleted and satisfy predicates
    if (!tp.first.is_deleted_ && (plan_->filter_predicate_ == nullptr ||
                                  plan_->filter_predicate_->Evaluate(&tp.second, table_info_->schema_).GetAs<bool>())) {
      break;
    }
    rid_it_++;
  }
  if (rid_it_ >= rids_.size()) {
    return false;
  }
  *tuple = tp.second;
  *rid = rids_[rid_it_++];
  return true;
}

}  // namespace bustub
