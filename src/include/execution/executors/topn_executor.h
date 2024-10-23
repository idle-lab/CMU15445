//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** A simplified heap for top n executor. */
class SimpleTopNHeap {
 public:
  explicit SimpleTopNHeap(const TopNPlanNode *plan) {
    comparator_ = [plan](const Tuple &left, const Tuple &right) -> bool {
      for (auto &x : plan->GetOrderBy()) {
        auto left_value = x.second->Evaluate(&left, plan->OutputSchema());
        auto rigth_value = x.second->Evaluate(&right, plan->OutputSchema());
        if (left_value.CompareEquals(rigth_value) == CmpBool::CmpTrue) {
          continue;
        }
        if (((x.first == OrderByType::DEFAULT || x.first == OrderByType::ASC) &&
             left_value.CompareLessThan(rigth_value) == CmpBool::CmpTrue) ||
            (x.first == OrderByType::DESC && left_value.CompareGreaterThan(rigth_value) == CmpBool::CmpTrue)) {
          return true;
        }
        break;
      }
      return false;
    };
  }

  void Push(Tuple &&a) {
    heap_.emplace_back(std::move(a));
    AdjustUp(heap_.size() - 1);
  }

  auto Pop() -> Tuple {
    std::swap(heap_[0], heap_.back());
    Tuple res = std::move(heap_.back());
    heap_.pop_back();
    AdjustDown(0);
    return res;
  }

  auto Top() -> Tuple & { return heap_[0]; }
  auto Size() -> size_t { return heap_.size(); }
  auto Empty() -> bool { return heap_.empty(); }
  void Clear() { heap_.clear(); }

  auto Compare(const Tuple &left, const Tuple &right) -> bool { return comparator_(left, right); }

 private:
  void AdjustUp(size_t idx) {
    for (; idx != 0U;) {
      size_t fa = ((idx + 1) >> 1) - 1;
      if (comparator_(heap_[idx], heap_[fa])) {
        return;
      }
      std::swap(heap_[fa], heap_[idx]);
      idx = fa;
    }
  }

  void AdjustDown(size_t idx) {
    for (; (idx << 1) + 1 < heap_.size();) {
      size_t m_son = (idx << 1) + 1;
      if ((idx << 1) + 2 < heap_.size() && comparator_(heap_[m_son], heap_[(idx << 1) + 2])) {
        m_son = (idx << 1) + 2;
      }
      if (comparator_(heap_[idx], heap_[m_son])) {
        std::swap(heap_[idx], heap_[m_son]);
        idx = m_son;
      } else {
        idx = m_son;
      }
    }
  }

  std::vector<Tuple> heap_{};
  std::function<bool(const Tuple &, const Tuple &)> comparator_;
};

/**
 *
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  SimpleTopNHeap heap_;
  std::list<Tuple> result_;
};
}  // namespace bustub
