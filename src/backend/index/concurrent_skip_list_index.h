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

#pragma once

#include <map>

#include "backend/index/index.h"
#include "backend/index/index_key.h"
#include "cds/container/skip_list_map_nogc.h"


namespace peloton {
namespace index {


//template <std::size_t KeySize> struct skip_list_key_comparator {
//  skip_list_key_comparator(index::IndexMetadata *metadata)
//  : schema(metadata->GetKeySchema()) {}
//
//  inline bool operator()(const GenericKey<KeySize> &lhs,
//                         const GenericKey<KeySize> &rhs) const {
//    /*
//    storage::Tuple lhTuple(schema);
//    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
//    storage::Tuple rhTuple(schema);
//    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
//    // lexicographical compare could be faster for fixed N
//    int diff = lhTuple.Compare(rhTuple);
//    return diff < 0;
//    */
//
//    for (oid_t column_itr = 0; column_itr < schema->GetColumnCount();
//         column_itr++) {
//      const Value lhs_value = lhs.ToValueFast(schema, column_itr);
//      const Value rhs_value = rhs.ToValueFast(schema, column_itr);
//
//      int diff = lhs_value.Compare(rhs_value);
//
//      if (diff) {
//        return diff < 0;
//      }
//    }
//
//
//
//  const catalog::Schema *schema;
//};

//template <class KeyComparator>
//struct peloton_skip_list_traits : public cds::container::skip_list::traits {
//  typedef KeyComparator compare;
//};

template <typename KeyType, typename ValueType, class KeyComparator>
class ConcurrentSkipListIndex : Index {
  friend class IndexFactory;


  typedef cds::container::SkipListMap<cds::gc::nogc, KeyType, ValueType> MapType;


 public:
  ConcurrentSkipListIndex(IndexMetadata *metadata);

  ~ConcurrentSkipListIndex() {}

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer> &);

  void ScanAllKeys(std::vector<ItemPointer> &);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer> &);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer *> &result);

  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer *> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return 0; }

 private:
  // KeyComparator
  // KeyComparator comparator;

  MapType container;
};
}
}
