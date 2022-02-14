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

namespace badgerdb
{

  constexpr int HASHTABLE_SZ(int bufs)
  {
    return ((int)(bufs * 1.2) & -2) + 1;
  }

  //----------------------------------------
  // Constructor of the class BufMgr
  //----------------------------------------

  BufMgr::BufMgr(std::uint32_t bufs)
      : numBufs(bufs),
        hashTable(HASHTABLE_SZ(bufs)),
        bufDescTable(bufs),
        bufPool(bufs)
  {
    for (FrameId i = 0; i < bufs; i++)
    {
      bufDescTable[i].frameNo = i;
      bufDescTable[i].valid = false;
    }

    clockHand = bufs - 1;
  }

  void BufMgr::advanceClock() {}

  void BufMgr::allocBuf(FrameId &frame) {}

  void BufMgr::readPage(File &file, const PageId pageNo, Page *&page)
  {

    //check if the page is already in buffer pool by invoking the BufHashTbl::lookup method on the hashtable to get a frame number.
    //This may throw a HashNotFoundException so be ready to catch it.

    FrameId frameNo; //TODO - Determine what starting value should be. (-1,0,1, NULL).

    try
    {

      //returns frameNo by reference
      this->hashTable.lookup(file, pageNo, frameNo);
    }
    catch (HashNotFoundException)
    {
      //Case 1: hash is not found in the hashTable

      //  -Call allocBuf() to allocate a buffer frame
      allocBuf(frameNo);

      //  -Then, call file.readPage() to read the page from disk to buffer bool frame.
      Page readPage = file.readPage(pageNo);
      bufPool[frameNo] = readPage;
      //should catch INVALIDPAGEEXCEPTION (#TODO: Confirm and implement catch)

      //  -Next, insert the page into the hashTable
      hashTable.insert(file, pageNo, frameNo);

      //  -Finally, invoke Set() on the frame to set ut up properly
      //          +Set() will automatically leave the pinCnt = 1
      bufDescTable[frameNo].Set(file, pageNo);

      //  -Return (kind of) a pointer to the frame containing the page via the page paramter
      page = &(bufPool[frameNo]);
      return;
    }

    // Case 2:
    // else, a HashNotFoundException was NOT caught
    // so, the Page is in the buffer pool
    // Note, the frameNo was likely changed by reference in hashTable.lookUp(...);
    // frameNo now will point to the frame to access.

    //set the appropraiate refbit
    bufDescTable[frameNo].refbit = true; //set to refbit to true/1

    //increment the pinCnt for the page
    (bufDescTable[frameNo].pinCnt)++;

    //return a pointer to the frame containing the page via the page parameter
    // to do so, set page = the address of bufPool[#]
    page = &(bufPool[frameNo]);

    return;
  }

  void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty)
  {

    FrameId frameNo;

    try
    {
      hashTable.lookup(file, pageNo, frameNo);
    }
    catch (HashNotFoundException)
    {

      //does nothing if the page is not found in the hash table
      return;

    } //else, page was found in the hash table.

    //if dirty is true, sets the dirty bit to true
    if (dirty)
    {
      bufDescTable[frameNo].dirty = true;
    }

    //pinCnt logic
    if (bufDescTable[frameNo].pinCnt > 0)
    {
      //if the pinCnt is larger than 0, then decrement pinCnt by 1
      bufDescTable[frameNo].pinCnt = bufDescTable[frameNo].pinCnt - 1;
    }
    else
    { // else, pinCnt < 1 and it cant be decremented. throw PageNotPinnedException

      std::string nameIn = file.filename();
      
      //Exception format: (const std::string &nameIn, PageId pageNoIn, FrameId frameNoIn);
      throw PageNotPinnedException(nameIn, pageNo, frameNo);
      
    }

    return;
  }

  void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page) {}

  void BufMgr::flushFile(File &file) {}

  void BufMgr::disposePage(File &file, const PageId PageNo) {}

  void BufMgr::printSelf(void)
  {
    int validFrames = 0;

    for (FrameId i = 0; i < numBufs; i++)
    {
      std::cout << "FrameNo:" << i << " ";
      bufDescTable[i].Print();

      if (bufDescTable[i].valid)
        validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
  }

} // namespace badgerdb