#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto CollectUndoLogs(TransactionManager* transaction_manager, const RID& rid, timestamp_t read_ts) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> undo_logs;
  auto is_exists = false;
  auto cur = transaction_manager->GetUndoLink(rid);
  while (cur.has_value() && cur->IsValid()) {
    auto undo_log = transaction_manager->GetUndoLog(cur.value());
    undo_logs.emplace_back(undo_log);
    cur = undo_logs.back().prev_version_;
    if (undo_log.ts_ <= read_ts) {
      is_exists = true;
      break;
    }
  }
  return is_exists ? std::make_optional<std::vector<UndoLog>>(undo_logs) : std::nullopt;
}

void Modify(std::vector<Value>& values, const Schema* partial_schema, const UndoLog& undo_log) {
  size_t undo_idx = 0;
  for (size_t i = 0;i < undo_log.modified_fields_.size();++i) {
    if (!undo_log.modified_fields_[i]) {
      continue;
    }
    values[i] = undo_log.tuple_.GetValue(partial_schema, undo_idx++);
  }
}

auto GetUndoLogSchema(const Schema* schema, const UndoLog& undo_log) -> Schema {
  std::vector<Column> partial_cols;
  for (size_t i = 0;i < undo_log.modified_fields_.size();++i) {
    if (!undo_log.modified_fields_[i]) {
      continue;
    }
    partial_cols.emplace_back(schema->GetColumn(i));
  }
  return Schema{partial_cols};
}


auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  auto is_delete = base_meta.is_deleted_;
  std::vector<Value> values;
  for (size_t i = 0;i < schema->GetColumnCount();++i) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }

  for (const auto& undo_log : undo_logs) {
    auto partial_schema = GetUndoLogSchema(schema, undo_log);
    is_delete = undo_log.is_deleted_;
    Modify(values, &partial_schema, undo_log);
  }

  return is_delete ? std::nullopt : std::make_optional<Tuple>(values, schema);
}

auto IsWriteWriteConflict(Transaction* txn, timestamp_t tuple_ts) -> bool {
  // 元组的时间戳大于事务读取时间戳且不等于事务时间戳
  if (tuple_ts != txn->GetTransactionTempTs() && txn->GetReadTs() < tuple_ts) {
    return true;
  }
  return false; 
}

auto GenerateDiffLog(const Schema* schema, const Tuple* new_tuple, const Tuple* old_tuple) -> UndoLog {
  size_t col_num = schema->GetColumnCount();
  std::vector<bool> modified_fields(col_num);

  std::vector<Value> values;
  std::vector<Column> cols;

  for (size_t i = 0;i < col_num;++i) {
    if (new_tuple != nullptr && new_tuple->GetValue(schema, i).CompareExactlyEquals(old_tuple->GetValue(schema, i))) {
      continue;
    }
    modified_fields[i] = true;
    values.emplace_back(old_tuple->GetValue(schema, i));
    cols.emplace_back(schema->GetColumn(i));
  }
  Schema delta_schema{cols};
  return UndoLog{false, modified_fields, Tuple{values, &delta_schema}};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  for (auto it = table_info->table_->MakeIterator();!it.IsEnd();++it) {
    fmt::print(stderr, "RID={}/{} ", it.GetRID().GetPageId(), it.GetRID().GetSlotNum());
    fmt::print(stderr, "ts=");
    if (it.GetTuple().first.ts_ >= TXN_START_ID) {
      fmt::print(stderr, "txn");
    }
    fmt::print(stderr, "{} ", it.GetTuple().first.ts_ & (TXN_START_ID - 1));
    if (it.GetTuple().first.is_deleted_) {
      fmt::print(stderr, "<del marker> ");
    }
    fmt::print(stderr, "tuple=(");
    for (size_t i = 0;i < table_info->schema_.GetColumnCount();++i) {
      if (it.GetTuple().second.GetValue(&table_info->schema_, i).IsNull()) {
        fmt::print(stderr, "<NULL>");
      } else {
        fmt::print(stderr, "{}", it.GetTuple().second.GetValue(&table_info->schema_, i).ToString());
      }
      if (i != table_info->schema_.GetColumnCount() - 1) {
        fmt::print(stderr, ", ");
      }
    }
    fmt::print(stderr, ") \n");

    auto cur = txn_mgr->GetUndoLink(it.GetRID());
    while (cur.has_value() && cur->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(cur.value());
      if (!undo_log_opt.has_value()) {
        break;
      }
      auto undo_log = undo_log_opt.value();
      fmt::print(stderr, "\ttxn{}_{} ", cur->prev_txn_ & (TXN_START_ID - 1), cur->prev_log_idx_);
      size_t undo_idx = 0;
      if (undo_log.is_deleted_) {
        fmt::print(stderr, "<del> ");
      } else {
        auto schema = GetUndoLogSchema(&table_info->schema_, undo_log);
        fmt::print(stderr, "(");
        for (size_t i = 0;i < undo_log.modified_fields_.size();++i) {
          if (undo_log.modified_fields_[i]) {
            if (undo_log.tuple_.GetValue(&schema, undo_idx).IsNull()) {
              fmt::print(stderr, "<NULL>");
            } else {
              fmt::print(stderr, "{}", undo_log.tuple_.GetValue(&schema, undo_idx).ToString());
            }
            undo_idx++;
          } else {
            fmt::print(stderr, "_");
          }
          if (i != table_info->schema_.GetColumnCount() - 1) {
            fmt::print(stderr, ", ");
          }
        }
        fmt::print(stderr, ") ");
      }
      fmt::print(stderr, "ts={}\n", undo_log.ts_);
      cur = undo_log.prev_version_;
    }
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
