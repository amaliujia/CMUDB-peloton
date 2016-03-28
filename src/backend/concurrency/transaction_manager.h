//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <unordered_map>

#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/concurrency/transaction.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace concurrency {

// class Transaction;

extern thread_local Transaction *current_txn;

class TransactionManager {
 public:
  TransactionManager() {
    next_txn_id_ = ATOMIC_VAR_INIT(START_TXN_ID);
    next_cid_ = ATOMIC_VAR_INIT(START_CID);
  }

  virtual ~TransactionManager() {}

  txn_id_t GetNextTransactionId() { return next_txn_id_++; }

  cid_t GetNextCommitId() { return next_cid_++; }

  virtual bool IsVisible(const txn_id_t &tuple_txn_id,
                         const cid_t &tuple_begin_cid,
                         const cid_t &tuple_end_cid) = 0;

  virtual bool IsOwner(storage::TileGroup *tile_group,
                       const oid_t &tuple_id) = 0;

  virtual bool IsAccessable(storage::TileGroup *tile_group,
                            const oid_t &tuple_id) = 0;

  virtual bool PerformRead(const oid_t &tile_group_id,
                           const oid_t &tuple_id) = 0;

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location) = 0;

  virtual bool AcquireTuple(storage::TileGroup *tile_group,
                            const oid_t &physical_tuple_id) = 0;

  virtual bool PerformInsert(const oid_t &tile_group_id,
                             const oid_t &tuple_id) = 0;

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location) = 0;

  virtual void SetInsertVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  virtual void SetDeleteVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  virtual void SetUpdateVisibility(const oid_t &tile_group_id,
                                   const oid_t &tuple_id) = 0;

  void SetTransactionResult(const Result result) {
    current_txn->SetResult(result);
  }

  Transaction *BeginTransaction() {
    Transaction *txn =
        new Transaction(GetNextTransactionId(), GetNextCommitId());
    current_txn = txn;
    return txn;
  }

  virtual Result CommitTransaction() = 0;

  virtual Result AbortTransaction() = 0;

  void ResetStates() {
    next_txn_id_ = START_TXN_ID;
    next_cid_ = START_CID;
  }

 private:
  std::atomic<txn_id_t> next_txn_id_;
  std::atomic<cid_t> next_cid_;
};
}  // End storage namespace
}  // End peloton namespace
