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

template <typename KeyType, typename ValueType, class KeyComparator>
ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::
          ConcurrentSkipListIndex(IndexMetadata *metadata) :
            Index(metadata),
            comparator(metadata) {

  }


template <typename KeyType, typename ValueType, class KeyComparator>
bool ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::InsertEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location) {

  KeyType index_key;
  index_key.SetFromKey(key);

  //container.insert(key, new ItemPointer(location));

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::DeleteEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location) {

  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::CondInsertEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE const ItemPointer &location,
  UNUSED_ATTRIBUTE std::function<bool(const ItemPointer &)> predicate) {

  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::Scan(
  UNUSED_ATTRIBUTE const std::vector<Value> &values,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
  UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction, std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::ScanAllKeys(
  UNUSED_ATTRIBUTE std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::ScanKey(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE std::vector<ItemPointer> &) {}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::Scan(
  UNUSED_ATTRIBUTE const std::vector<Value> &values,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
  UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::ScanAllKeys(
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator>
void ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::ScanKey(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator>
std::string ConcurrentSkipListIndex<KeyType, ValueType, KeyComparator>::GetTypeName() const {
  return "CONCURRENTSKIPLIST";
}

// Explicit template instantiation
template class ConcurrentSkipListIndex<IntsKey<1>, ItemPointer *, IntsComparator<1>>;
template class ConcurrentSkipListIndex<IntsKey<2>, ItemPointer *, IntsComparator<2>>;
template class ConcurrentSkipListIndex<IntsKey<3>, ItemPointer *, IntsComparator<3>>;
template class ConcurrentSkipListIndex<IntsKey<4>, ItemPointer *, IntsComparator<4>>;

template class ConcurrentSkipListIndex<GenericKey<4>, ItemPointer *, IntsComparator<4>>;
template class ConcurrentSkipListIndex<GenericKey<8>, ItemPointer *, IntsComparator<8>>;
template class ConcurrentSkipListIndex<GenericKey<12>, ItemPointer *, IntsComparator<12>>;
template class ConcurrentSkipListIndex<GenericKey<16>, ItemPointer *, IntsComparator<16>>;
template class ConcurrentSkipListIndex<GenericKey<24>, ItemPointer *, IntsComparator<24>>;
template class ConcurrentSkipListIndex<GenericKey<32>, ItemPointer *, IntsComparator<32>>;
template class ConcurrentSkipListIndex<GenericKey<48>, ItemPointer *, IntsComparator<48>>;
template class ConcurrentSkipListIndex<GenericKey<64>, ItemPointer *, IntsComparator<64>>;
template class ConcurrentSkipListIndex < GenericKey<96>, ItemPointer *, IntsComparator<96>>;
template class ConcurrentSkipListIndex<GenericKey<128>, ItemPointer *, IntsComparator<128>>;
template class ConcurrentSkipListIndex<GenericKey<256>, ItemPointer *, IntsComparator<246>>;
template class ConcurrentSkipListIndex<GenericKey<512>, ItemPointer *, IntsComparator<512>>;

template class ConcurrentSkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator>;

}  // namespace index
}  // namespace peloton