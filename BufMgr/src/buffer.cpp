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

void BufMgr::advanceClock() {

    //clockHand is undefined in the beginning
  if (clockHand == NULL)
    clockHand = 0;
    //modular arithmetic to move around clock
  clockHand = (clockHand+1) % bufMgr.numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
  int count = 0;//counter for amount of frames with > 0 pinCnts
  for (int i = 0; i < numBufs; i++){
      if (bufDescTable[i].pinCnt > 0)
        count +=1; //increment count
    }
    if (count == numBufs - 1) {
      throw BadBufferException(clockHand, bufDescTable[clockHand].dirty,
      bufDescTable[clockHand].valid, bufDescTable[clockHand].refbit);
    }
  //allocate free frame from clock algo
  while (true){
    
    if (bufDescTable[clockHand].refbit == false && bufDescTable[clockHand].pinCnt == 0){
    // allocate this frame
      if (bufDescTable[clockHand].valid == true){
        //remove from hashtable
        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      }

      if (bufDescTable[clockHand].dirty == true){
        //writes page back to disk
        flushFile(bufDescTable[clockHand].file);
      }

      frame = &(clockHand);
      break; //leave while loop
    }

    else {
      advanceClock();
    }
  }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
