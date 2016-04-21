//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.h
//
// Identification: src/backend/executor/seq_scan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/barrier.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/exchange_seq_scan_plan.h"
#include "backend/executor/abstract_scan_executor.h"

namespace peloton {
namespace executor {

class SeqScanExecutor : public AbstractScanExecutor {
 public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor &operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor &operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  void ThreadExecute(oid_t assigned_tile_group_offset_start,
                     oid_t assigned_tile_group_offset_end,
                     std::vector<LogicalTile *> *buffer,
                     peloton::Barrier *barrier);

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  /** @brief flag to show if do parallel scan. */
  bool if_parallel_ = false;

  /** @brief flag to show if parallel scan preparation done. */
  bool parallel_done_ = false;

  /** @brief Used to buffer theads' output */
  std::deque<LogicalTile *> buffered_output_tiles;

};

}  // namespace executor
}  // namespace peloton
