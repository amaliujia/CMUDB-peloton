//
// Created by Rui Wang on 16-4-4.
//

#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/hash_plan.h"
#include "backend/planner/hash_join_plan.h"
#include "backend/planner/exchange_hash_plan.h"
#include "backend/planner/exchange_hash_join_plan.h"
#include "backend/planner/exchange_seq_scan_plan.h"
#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

typedef const expression::AbstractExpression HashKeyType;
typedef std::unique_ptr<HashKeyType> HashKeyPtrType;

static planner::AbstractPlan *BuildParallelHashPlan(const planner::AbstractPlan *hash_plan) {
  LOG_INFO("Mapping hash plan to parallel hash plan");

  const planner::HashPlan *plan = dynamic_cast<const planner::HashPlan *>(hash_plan);
  const auto& hash_keys = plan->GetHashKeys();
  std::vector<HashKeyPtrType> copied_hash_keys;
    for (const auto &key : hash_keys) {
      const expression::AbstractExpression *temp_key = key->Copy();
      copied_hash_keys.push_back(std::unique_ptr<HashKeyType>(temp_key));
    }

  return new planner::ExchangeHashPlan(copied_hash_keys);
}

static planner::AbstractPlan *BuildParallelSeqScanPlan(
    const planner::AbstractPlan *seq_scan_plan) {
  /* Grab the target table */
  LOG_INFO(
      "Mapping seq scan plan to parallel seq scan plan");
  const planner::SeqScanPlan *plan =
      dynamic_cast<const planner::SeqScanPlan *>(seq_scan_plan);
  planner::AbstractPlan *exchange_seq_scan_plan =
      new planner::ExchangeSeqScanPlan(plan);
  return exchange_seq_scan_plan;
}

static planner::AbstractPlan *BuildParalleHashJoinPlan(
  const planner::AbstractPlan *hash_join_plan) {
  LOG_INFO(
    "Mapping hash join plan to parallel hash join plan");
  const planner::HashJoinPlan *plan =
    dynamic_cast<const planner::HashJoinPlan *>(hash_join_plan);

  std::shared_ptr<const catalog::Schema> shared_schema(plan->GetSchema());
  const std::vector<oid_t> outer_column_ids = plan->GetOuterHashIds();
 
  planner::AbstractPlan *exchange_hash_join_plan =
        new planner::ExchangeHashJoinPlan(plan->GetJoinType(),
                                          plan->GetPredicate()->Copy(),
                                          plan->GetProjInfo()->Copy(),
                                          shared_schema,
                                          outer_column_ids);
  return exchange_hash_join_plan;
}

static planner::AbstractPlan *BuildParallelPlanUtil(
    const planner::AbstractPlan *old_plan) {
  switch (old_plan->GetPlanNodeType()) {
    case PLAN_NODE_TYPE_SEQSCAN:
      return BuildParallelSeqScanPlan(old_plan);
    case PLAN_NODE_TYPE_HASH:
      return BuildParallelHashPlan(old_plan);
    case PLAN_NODE_TYPE_HASHJOIN:
      return BuildParalleHashJoinPlan(old_plan);
    default:
      return old_plan->Copy().release();
  }
}

/**
 * There are two ways to do such mapping.
 * 1. Plan level parallelism. For each type of plan, has one function to do
 *mapping.
 * 2. Plan node level parallelism. For each type of node, has one function to do
 *mapping.
 *
 * Here second solution is adopted.
 */
const planner::AbstractPlan *PlanTransformer::BuildParallelPlan(
    const planner::AbstractPlan *old_plan) {
  LOG_TRACE("Mapping single-threaded plan to parallel plan");

  // Base case: leaf plan node
  if (old_plan->GetChildren().size() == 0) {
    return BuildParallelPlanUtil(old_plan);
  } else {
    planner::AbstractPlan *ret_ptr = nullptr;
    std::vector<planner::AbstractPlan *> child_plan_vec;

    for (const auto&child : old_plan->GetChildren()) {
      child_plan_vec.push_back(BuildParallelPlanUtil(child.get()));
    }

    ret_ptr = BuildParallelPlanUtil(old_plan);
    for (auto child_plan : child_plan_vec) {
      ret_ptr->AddChild(std::unique_ptr<planner::AbstractPlan>(child_plan));
    }

    return ret_ptr;
  }
}

}  // namespace bridge
}  // namespace peloton
