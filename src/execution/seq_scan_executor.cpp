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
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  tit_ = table_info->table_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tit_.IsEnd()) {
    return false;
  }
  auto tp = tit_.GetTuple();
  while (!tit_.IsEnd() && tp.first.is_deleted_) {
    tp = tit_.GetTuple();
    ++tit_;
  }
  if (tit_.IsEnd()) {
    return false;
  }
  *tuple = tit_.GetTuple().second;
  *rid = tit_.GetRID();
  ++tit_;
  return true;
}

}  // namespace bustub
