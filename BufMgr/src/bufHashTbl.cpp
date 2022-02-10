/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "bufHashTbl.h"

#include <functional>
#include <iostream>
#include <memory>

#include "buffer.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/hash_table_exception.h"

namespace badgerdb {

int BufHashTbl::hash(const File& file, const PageId pageNo) {
  auto hash =
      std::hash<std::string>{}(file.filename()) ^ std::hash<PageId>{}(pageNo);
  return hash % HTSIZE;
}

BufHashTbl::BufHashTbl(int htSize) : HTSIZE(htSize), ht(htSize) {
  // allocate an array of pointers to hashBuckets
}

void BufHashTbl::insert(const File& file, const PageId pageNo,
                        const FrameId frameNo) {
  int index = hash(file, pageNo);

  std::shared_ptr<hashBucket> tmpBuc = ht[index];
  while (tmpBuc) {
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo)
      throw HashAlreadyPresentException(tmpBuc->file.filename(), tmpBuc->pageNo,
                                        tmpBuc->frameNo);
    tmpBuc = tmpBuc->next;
  }

  tmpBuc = std::make_shared<hashBucket>();
  if (!tmpBuc) throw HashTableException();

  tmpBuc->file = file;
  tmpBuc->pageNo = pageNo;
  tmpBuc->frameNo = frameNo;
  tmpBuc->next = ht[index];
  ht[index] = tmpBuc;
}

void BufHashTbl::lookup(const File& file, const PageId pageNo,
                        FrameId& frameNo) {
  int index = hash(file, pageNo);
  std::shared_ptr<hashBucket> tmpBuc = ht[index];
  while (tmpBuc) {
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo) {
      frameNo = tmpBuc->frameNo;  // return frameNo by reference
      return;
    }
    tmpBuc = tmpBuc->next;
  }

  throw HashNotFoundException(file.filename(), pageNo);
}

void BufHashTbl::remove(const File& file, const PageId pageNo) {
  int index = hash(file, pageNo);
  std::shared_ptr<hashBucket> tmpBuc = ht[index];
  std::shared_ptr<hashBucket> prevBuc;

  while (tmpBuc) {
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo) {
      if (prevBuc)
        prevBuc->next = tmpBuc->next;
      else
        ht[index] = tmpBuc->next;

      tmpBuc.reset();
      return;
    } else {
      prevBuc = tmpBuc;
      tmpBuc = tmpBuc->next;
    }
  }

  throw HashNotFoundException(file.filename(), pageNo);
}

}  // namespace badgerdb
