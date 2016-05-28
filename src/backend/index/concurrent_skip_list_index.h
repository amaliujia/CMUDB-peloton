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

#include "backend/index/index.h"

#include "cds/container/skip_list_map_nogc.h"

#define CDS_DOXYGEN_INVOKED 1

namespace peloton {
namespace index {


template <typename KeyType, typename ValueType>
class ConcurrentSkipListIndex : Index {
  friend class IndexFactory;

  typedef cds::container::SkipListMap<KeyType, ValueType> MapType;
public:
  ConcurrentSkipListIndex(IndexMetadata *metadata) :Index(metadata) {

  }

  ~ConcurrentSkipListIndex() {

  }

private:

  MapType container_;
};

}
}




