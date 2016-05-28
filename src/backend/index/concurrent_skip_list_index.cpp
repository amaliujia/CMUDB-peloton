//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.h
//
// Identification: src/backend/index/bwtree_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrent_skip_list_index.h"
#include "backend/index/btree_index.h"
#include "backend/index/index_key.h"


namespace peloton {
namespace index {

template <typename KeyType, typename ValueType>
bool ConcurrentSkipListIndex<KeyType, ValueType>::InsertEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location) {

  return false;
}

template <typename KeyType, typename ValueType>
bool ConcurrentSkipListIndex<KeyType, ValueType>::DeleteEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location) {

  return false;
}

template <typename KeyType, typename ValueType>
bool ConcurrentSkipListIndex<KeyType, ValueType>::CondInsertEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location,
  UNUSED_ATTRIBUTE std::function<bool(const ItemPointer &)> predicate) {

  return false;
}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::Scan(
  UNUSED_ATTRIBUTE const std::vector<Value> &values,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
  UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction, std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::ScanAllKeys(
  UNUSED_ATTRIBUTE std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::ScanKey(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::Scan(
  UNUSED_ATTRIBUTE const std::vector<Value> &values,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
  UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::ScanAllKeys(
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType>
void ConcurrentSkipListIndex<KeyType, ValueType>::ScanKey(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType>
std::string ConcurrentSkipListIndex<KeyType, ValueType>::GetTypeName() const {
  return "CONCURRENTSKIPLIST";
}

// Explicit template instantiation
template class ConcurrentSkipListIndex<IntsKey<1>, ItemPointer *>;
template class ConcurrentSkipListIndex<IntsKey<2>, ItemPointer *>;
template class ConcurrentSkipListIndex<IntsKey<3>, ItemPointer *>;
template class ConcurrentSkipListIndex<IntsKey<4>, ItemPointer *>;

template class ConcurrentSkipListIndex<GenericKey<4>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<8>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<12>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<16>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<24>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<32>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<48>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<64>, ItemPointer *>;
template class ConcurrentSkipListIndex < GenericKey<96>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<128>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<256>, ItemPointer *>;
template class ConcurrentSkipListIndex<GenericKey<512>, ItemPointer *>;

template class ConcurrentSkipListIndex<TupleKey, ItemPointer *>;
}
}