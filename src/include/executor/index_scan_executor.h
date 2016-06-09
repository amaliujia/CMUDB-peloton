//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.h
//
// Identification: src/include/executor/index_scan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "executor/abstract_scan_executor.h"
#include "planner/index_scan_plan.h"

namespace peloton {

namespace storage {
class AbstractTable;
}

namespace executor {

class IndexScanExecutor : public AbstractScanExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

  ~IndexScanExecutor();

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Helper
  //===--------------------------------------------------------------------===//
  bool ExecPrimaryIndexLookup();
  bool ExecSecondaryIndexLookup();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result_;

  /** @brief Result itr */
  oid_t result_itr_ = INVALID_OID;

  /** @brief Computed the result */
  bool done_ = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief index associated with index scan. */
  index::Index *index_ = nullptr;

  const storage::AbstractTable *table_ = nullptr;

  std::vector<peloton::Value> values_;

  std::vector<oid_t> full_column_ids_;

  bool key_ready_ = false;
};

}  // namespace executor
}  // namespace peloton
