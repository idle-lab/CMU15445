//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

class WindowFunctionHelper {
 public:
  explicit WindowFunctionHelper(WindowFunctionType wf_type) : wf_type_(wf_type) { Init(); }

  /** Initialization WindowFunctionHelper */
  void Init() {
    switch (wf_type_) {
      case WindowFunctionType::CountStarAggregate: {
        cur_value_ = ValueFactory::GetZeroValueByType(TypeId::INTEGER);
        break;
      }
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::Rank: {
        cur_value_ = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      }
    }
  }

  /** Combine value into cur_value_ */
  void Combine(const Value &value) {
    switch (wf_type_) {
      case WindowFunctionType::CountStarAggregate: {
        cur_value_ = cur_value_.Add(ValueFactory::GetIntegerValue(1));
        break;
      }
      case WindowFunctionType::CountAggregate: {
        if (value.IsNull()) {
          return;
        }
        if (cur_value_.IsNull()) {
          cur_value_ = ValueFactory::GetZeroValueByType(TypeId::INTEGER);
        }
        cur_value_ = cur_value_.Add(ValueFactory::GetIntegerValue(1));
        break;
      }
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::Rank: {
        if (value.IsNull()) {
          return;
        }
        if (cur_value_.IsNull()) {
          cur_value_ = ValueFactory::GetZeroValueByType(value.GetTypeId());
        }
        cur_value_ = cur_value_.Add(value);
        break;
      }
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::MinAggregate: {
        if (value.IsNull()) {
          return;
        }
        if (cur_value_.IsNull()) {
          cur_value_ = value;
        } else {
          cur_value_ = (wf_type_ == WindowFunctionType::MaxAggregate ? cur_value_.Max(value) : cur_value_.Min(value));
        }
        break;
      }
    }
  }

  /** Get current aggregation result. */
  auto Get() -> Value & { return cur_value_; }

 private:
  Value cur_value_;
  WindowFunctionType wf_type_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto IsEqual(const std::vector<Value> &left, const std::vector<Value> &right) -> bool {
    for (size_t i = 0; i < left.size(); ++i) {
      if (left[i].CompareEquals(right[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> result_;
  size_t it_;
};
}  // namespace bustub
