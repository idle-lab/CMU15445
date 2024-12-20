//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_.load());

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  const auto& ws = txn->GetWriteSets();


  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  // Iterate through all tuples changed by this transaction (using the write set),
  // set the timestamp of the base tuples to the commit timestamp. 
  for (const auto& [table_oid, write_set] : ws) {
    auto* table_info = catalog_->GetTable(table_oid);
    std::optional<VersionUndoLink> version_link;

    for (const auto& changed_tuple_rid : write_set) {
      auto tuple = table_info->table_->GetTuple(changed_tuple_rid);

      tuple.first.ts_ = commit_ts;
      table_info->table_->UpdateTupleInPlace(tuple.first, tuple.second, changed_tuple_rid);

      // update progress status.
      version_link = GetVersionLink(changed_tuple_rid);
      if (!version_link.has_value()) {
        continue;
      }
      version_link->in_progress_ = false;
      UpdateVersionLink(changed_tuple_rid, version_link);
    }
  }

  txn->state_ = TransactionState::COMMITTED;
  txn->commit_ts_ = commit_ts;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  ++last_commit_ts_;

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto lowest_ts = running_txns_.GetWatermark();
  std::unordered_map<txn_id_t, size_t> gc_cnt;

  for (auto & [txn_id, txn] : txn_map_) {
    if (txn->state_ != TransactionState::COMMITTED && txn->state_ != TransactionState::ABORTED) {
      continue;
    }

    if (txn->undo_logs_.size() == 0) {
      gc_cnt[txn_id] = 0;
    }

    for (auto [table_oid, rids/* must a copy */] : txn->GetWriteSets()) {
      auto* table_info = catalog_->GetTable(table_oid);

      // delete unaccessible undo log.
      for (auto& rid : rids) {
        auto meta = table_info->table_->GetTupleMeta(rid);
        bool is_accessible = (meta.ts_ > lowest_ts);
        auto opt = GetVersionLink(rid);
        if (!opt.has_value()) {
          continue;
        }
        auto cur = opt.value().prev_;

        while (true) {
          if (!is_accessible) {
            // 可能之前的 gc 中被删除
            auto it = txn_map_.find(cur.prev_txn_);
            if (it == txn_map_.end()) {
              break;
            }
            gc_cnt[cur.prev_txn_]++;
            it->second->write_set_[table_oid].erase(rid);
          }
          auto undo_log = GetUndoLogOptional(cur);
          if (!undo_log.has_value()) {
            break;
          }

          // Keep the first undo log that is less than or equal to lowest_ts
          is_accessible = (undo_log->ts_ > lowest_ts);
          cur = undo_log->prev_version_;
        }
      }
    }
  }

  for (auto& [txn_id, cnt] : gc_cnt) {
    auto txn = txn_map_[txn_id];
    if (txn->state_ != TransactionState::COMMITTED && txn->state_ != TransactionState::ABORTED) {
      continue;
    }
    if (txn->undo_logs_.size() == cnt) {
      txn_map_.erase(txn_id);
    }
  }
}

}  // namespace bustub
