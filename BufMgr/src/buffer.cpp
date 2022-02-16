/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {}

void BufMgr::allocBuf(FrameId& frame) {}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  // Test for commit
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

/**
   * Allocates a new, empty page in the file and returns the Page object.
   * The newly allocated page is also assigned a frame in the buffer pool.
   *
   * @param file   	File object
   * @param PageNo  Page number. The number assigned to the page in the file is
   * returned via this reference.
   * @param page  	Reference to page pointer. The newly allocated in-memory
   * Page object is returned via this reference.
   */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {

  // Allocate an empty page in the specified file 
  page = file.allocatePage();

  // Obtain a buffer pool frame (id passed via FrameId variable)
  FrameId frame;

  try {
    allocBuf(frame);
  } catch (BadBufferException e) {

    // no such buffer is found which can be allocated
    std::cout << e.message;
    return;
  }

  // insert entry into hash table
  // TODO: catch exceptions
  try {
    hashTable.insert(file, pageNo, frame);

  } catch (HashAlreadyPresentException e) {

    // the corresponding page already exists in the hash table
    std::cout << e.message;
    return;

  } catch (HashTableException) {

    // could not create a new bucket as running of memory
    std::cout << e.message;
    return;

  }
  // invoke Set() on the frame
  bufDescTable[frame].Set(file, pageNo);

  // return the page number and a pointer to the buffer frame allocated
  page = &(bufPool[frame]);
  PageNo = &(bufDescTable[frame].pageNo);
}

void BufMgr::flushFile(File& file) {}

  /**
   * Delete page from file and also from buffer pool if present.
   * Since the page is entirely deleted from file, its unnecessary to see if the
   * page is dirty.
   *
   * @param file   	File object
   * @param PageNo  Page number
   */
void BufMgr::disposePage(File& file, const PageId PageNo) {

  // Get the frame id (id passed via FrameId variable)
  FrameId frame;
  hashTable.lookup(file, pageNo, frame);

  // Check that page to be deleted is allocated a frame within buffer pool
  if(std::find(bufDescTable.begin(), bufDescTable.end(), frame) != bufPool.end()) {
    
    // Free the allocated frame
    bufDescTable[frame].clear();

    // Remove entry from hash table
    // TODO: catch exceptions
    try {
      hashTable.remove(file, pageNo);
    } catch (HashNotFoundException e) {
      std::cout << e.meesage;
    }

  }
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
