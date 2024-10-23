//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/* HashJoinKey rerepresents a key in hash join operation. */
struct HashJoinKey {
  // The equi-condition values.
  std::vector<Value> hash_key_;
  /* Compares two hash join keys for equality. */
  auto operator==(const HashJoinKey &other) const -> bool {
    if (hash_key_.size() != other.hash_key_.size()) {
      return false;
    }
    for (size_t i = 0; i < hash_key_.size(); ++i) {
      if (hash_key_[i].CompareEquals(other.hash_key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/* HashJoinValue rerepresents tuples that have the same HashJoinKey */
struct HashJoinValue {
  std::vector<Tuple> tuples_;
  bool matched_{false};
};

}  // namespace bustub

namespace std {

/* Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> size_t {
    size_t hash_res = 0;
    for (auto &key : hash_key.hash_key_) {
      hash_res = bustub::HashUtil::CombineHashes(hash_res, bustub::HashUtil::HashValue(&key));
    }
    return hash_res;
  }
};

}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as a right HashJoinKey */
  auto MakeRightHashJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {values};
  }

  /** @return The tuple as a left HashJoinKey */
  auto MakeLeftHashJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {values};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  std::unordered_map<HashJoinKey, HashJoinValue> ht_{};

  // join context
  HashJoinValue *now_value_{nullptr};
  Tuple right_tuple_;
  RID right_rid_;
  size_t value_it_;
  bool is_over_{false};
  bool is_right_over_{false};
};

}  // namespace bustub
