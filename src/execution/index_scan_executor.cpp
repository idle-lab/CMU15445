//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> values{plan_->pred_key_->val_};
  Tuple tp{values, index_info_->index_->GetKeySchema()};
  rids_.clear();
  rid_it_ = 0;
  htable->ScanKey(tp, &rids_, exec_ctx_->GetTransaction());

  // Optimization: Index Scan Page Sorting
  sort(rids_.begin(), rids_.end(), [](const RID &a, const RID &b) {
    if (a.GetPageId() == b.GetPageId()) {
      return a.GetSlotNum() < b.GetSlotNum();
    }
    return a.GetSlotNum() < b.GetSlotNum();
  });
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta tp_mate;
  while (rid_it_ < rids_.size()) {
    tp_mate = table_info_->table_->GetTupleMeta(rids_[rid_it_]);
    if (!tp_mate.is_deleted_) {
      break;
    }
    rid_it_++;
  }
  // Can't find entry
  if (rid_it_ >= rids_.size()) {
    return false;
  }

  *tuple = table_info_->table_->GetTuple(rids_[rid_it_]).second;
  *rid = rids_[rid_it_++];
  return true;
}

}  // namespace bustub
