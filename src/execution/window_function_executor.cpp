#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
    child_executor_->Init();
    result_.clear();

    std::vector<Tuple> tuples;
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple,&rid)) {
        tuple.SetRid(rid);
        tuples.emplace_back(std::move(tuple));
        // Allocate space in advance
        result_[rid].reserve(plan_->columns_.size());
    }
    auto& tuple_schema = child_executor_->GetOutputSchema();
    for (size_t i = 0; i < plan_->columns_.size(); ++i) {
        auto wf_ptr = plan_->window_functions_.find(i);
        if (wf_ptr != plan_->window_functions_.end()) {
            const auto& wf = wf_ptr->second;
            auto wf_helper = std::make_unique<WindowFunctionHelper>(wf.type_);
            std::vector<std::pair<OrderByType,AbstractExpressionRef>> order_exprs;
            for(auto& expr : wf.partition_by_) {
                order_exprs.emplace_back(OrderByType::ASC, expr);
            }
            for(auto& expr : wf.order_by_) {
                order_exprs.emplace_back(OrderByType::ASC, expr);
            }
            SortTupleHelperFunction(tuples, order_exprs, tuple_schema);
            for (auto& tuple : tuples) {
                if ()
            }
        } else {
            for(auto& tuple : tuples) {
                result_[tuple.GetRid()].emplace_back(plan_->columns_[i]->Evaluate(&tuple, tuple_schema));
            }
        }
    }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }
}  // namespace bustub
