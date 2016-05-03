//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/backend/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/executor/seq_scan_executor.h"

#include <memory>
#include <utility>
#include <vector>


#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  const auto &root_node = GetPlanNode<planner::AbstractPlan>();
  // Grab data from plan node.
  assert(root_node.GetPlanNodeType() == PLAN_NODE_TYPE_SEQSCAN ||
          root_node.GetPlanNodeType() == PLAN_NODE_TYPE_EXCHANGE_SEQSCAN);
  if (root_node.GetPlanNodeType() == PLAN_NODE_TYPE_SEQSCAN ) {
    const auto &node = GetPlanNode<planner::SeqScanPlan>();
    target_table_ = node.GetTable();
  } else {
    const auto &node = GetPlanNode<planner::ExchangeSeqScanPlan>();
    target_table_ = node.GetTable();
    if_parallel_ = true;
  }

  current_tile_group_offset_ = START_OID;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  // Scanning over a logical tile.
  if (children_.size() == 1) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    assert(target_table_ == nullptr);
    assert(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          expression::ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
                  .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }
  }
  // Scanning a table
  else if (children_.size() == 0) {

    LOG_TRACE("Seq Scan executor :: 0 child ");

    assert(target_table_ != nullptr);
    assert(column_ids_.size() > 0);

    auto &transaction_manager =
        concurrency::TransactionManagerFactory::GetInstance();
    // Retrieve next tile group.
    if (if_parallel_ == false) {
      while (current_tile_group_offset_ < table_tile_group_count_) {
        auto tile_group =
          target_table_->GetTileGroup(current_tile_group_offset_++);
        auto tile_group_header = tile_group->GetHeader();

        oid_t active_tuple_count = tile_group->GetNextTupleSlot();


        // Construct position list by looping through tile group
        // and applying the predicate.
        std::vector<oid_t> position_list;
        for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

          ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

          // check transaction visibility
          if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
            // if the tuple is visible, then perform predicate evaluation.
            if (predicate_ == nullptr) {
              position_list.push_back(tuple_id);
              auto res = transaction_manager.PerformRead(location);
              if (!res) {
                transaction_manager.SetTransactionResult(RESULT_FAILURE);
                return res;
              }
            } else {
              expression::ContainerTuple<storage::TileGroup> tuple(
                tile_group.get(), tuple_id);
              auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
                .IsTrue();
              if (eval == true) {
                position_list.push_back(tuple_id);
                auto res = transaction_manager.PerformRead(location);
                if (!res) {
                  transaction_manager.SetTransactionResult(RESULT_FAILURE);
                  return res;

                }
              }
            }
          }

          // Don't return empty tiles
          if (position_list.size() == 0) {
            continue;
          }

          // Construct logical tile.
          std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
          logical_tile->AddColumns(tile_group, column_ids_);
          logical_tile->AddPositionList(std::move(position_list));

          SetOutput(logical_tile.release());
          return true;
        }
      }
    }else if (parallel_done_ == false){
      size_t num_worker = peloton::ThreadManager::GetInstance().GetNumThreads();
      
      size_t num_tasks = (table_tile_group_count_ >= num_worker ? num_worker : table_tile_group_count_ ); 
      size_t task_size = (table_tile_group_count_ / num_worker) + ((table_tile_group_count_ % num_worker) != 0 ? 1 : 0); 

      std::vector<std::vector<LogicalTile *>> buffers(num_worker, std::vector<LogicalTile *>());

      peloton::Barrier barrier(num_tasks);

      for (size_t i = 0; i < num_tasks; i++) {
        oid_t start = i * task_size;
        oid_t end = (i + 1) * task_size - 1;
        if (end > (table_tile_group_count_ - 1)) {
          end = table_tile_group_count_ - 1;
        }
        buffers[i].resize(end - start + 1);

        std::function<void()> f_seq_scan =
          std::bind(&SeqScanExecutor::ThreadExecute, this,
                    start, end, &buffers[i], &barrier);
        ThreadManager::GetInstance().AddTask(f_seq_scan);
      }

      barrier.Wait();

      // copy buffers to queue.
      for (const auto& v : buffers) {
        buffered_output_tiles.insert(buffered_output_tiles.end(), v.begin(), v.end());
      }
      LOG_INFO("%lu LogicalTile in buffer", buffered_output_tiles.size());
      parallel_done_ = true;
      return DExecute();
    } else {
      if (buffered_output_tiles.empty() == false) {
        auto output_tile = buffered_output_tiles.front();
        buffered_output_tiles.pop_front();

        if (output_tile != nullptr) {
          SetOutput(output_tile);
          return true;
        } else {
          return DExecute();
        }
      }
    }
  }

  return false;
}


void SeqScanExecutor::ThreadExecute(oid_t assigned_tile_group_offset_start,
                                    oid_t assigned_tile_group_offset_end,
                                    std::vector<LogicalTile *> *buffer,
                                    peloton::Barrier *barrier) {
//  LOG_INFO(
//    "Parallel worker :: executor: %s with assigned tile group range %lu %lu" ,
//    GetRawNode()->GetInfo().c_str(),
//    assigned_tile_group_offset_start,
//    assigned_tile_group_offset_end);

  auto &transaction_manager =
    concurrency::TransactionManagerFactory::GetInstance();
  // Each transaction has a thread local pointer to a transaction object.
  // If do not assign concurrency::current_txn here, transaction won't
  // worker in this thread.
  concurrency::current_txn = this->executor_context_->GetTransaction();

  for (oid_t i = assigned_tile_group_offset_start; i <= assigned_tile_group_offset_end; i++) {
    auto tile_group = target_table_->GetTileGroup(i);
    auto tile_group_header = tile_group->GetHeader();

    oid_t active_tuple_count = tile_group->GetNextTupleSlot();
    std::vector<oid_t> position_list;
    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      // check transaction visibility
      if (transaction_manager.IsVisible(tile_group_header, tuple_id)) {
        // if the tuple is visible, then perform predicate evaluation.
        if (predicate_ == nullptr) {
          position_list.push_back(tuple_id);

          // TODO: In current version of MVCC, one transaction only assumes one thread.
          // TODO: Therefore PerformRead is not thread safe.
          // TODO: Since parallel scan only used in OLAP query, assume there is no
          // TODO: transaction abort. But this assumption is weak, better to have a
          // TODO: transaction implememtation that supports multi-threading.
          // auto res = transaction_manager.PerformRead(tile_group->GetTileGroupId(),
          //                                          tuple_id);
        } else {
          expression::ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                               tuple_id);
          auto eval =
            predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
          if (eval == true) {
            position_list.push_back(tuple_id);
            // auto res = transaction_manager.PerformRead(
            //  tile_group->GetTileGroupId(), tuple_id);

          }
        }
      }
    }

    if (position_list.size() != 0) {
        // Construct logical tile.
        std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
        logical_tile->AddColumns(tile_group, column_ids_);
        logical_tile->AddPositionList(std::move(position_list));
        buffer->push_back(logical_tile.release());
    } else {
        buffer->push_back(nullptr);
    }
  }

  barrier->Notify();
}

}  // namespace executor
}  // namespace peloton
