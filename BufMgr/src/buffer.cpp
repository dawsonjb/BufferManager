/**
 * @file buffer.cpp
 * @author Collin Krause, student id: 9078082915, email: cpkrause2@wisc.edu
 * @author Dawson Bauer, student id: 9078157287, email: djbauer2@wisc.edu
 * @author Rex Wasserman, studentid: 9079428372, email: rwasserman2@wisc.edu
 * @date 2/16/2022
 *
 * @brief Contains the BufMgr class is the heart of the buffer manager. Contains
 * all the methods implemented by the students
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */


#include "buffer.h"

#include <iostream>
#include <memory>
#include <algorithm>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_table_exception.h"

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

  void BufMgr::advanceClock()
  {

    //clockHand is undefined in the beginning
    if (clockHand == NULL)
      clockHand = 0;

    //modular arithmetic to move around clock
    clockHand = (clockHand + 1) % numBufs;
  }

  void BufMgr::allocBuf(FrameId &frame)
  {

    //orginHand represents the first clockHand checked.
    FrameId originHand = clockHand;
    bool flag = false; //counter for amount of frames with > 0 pinCnts

    for (int i = 0; i < numBufs; i++)
    {
      if (bufDescTable[i].pinCnt == 0)
        flag = true; 
    }

    if (flag == false)
    {
      std::cout << "BufferExceededException() Thrown" << std::endl;
      throw BufferExceededException();
    }

    //allocate free frame from clock algo
    while (true)
    {

      if (bufDescTable[clockHand].refbit == false && bufDescTable[clockHand].pinCnt == 0)
      {
        // allocate this frame
        if (bufDescTable[clockHand].valid == true)
        {
          //remove from hashtable
          hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        }

        if (bufDescTable[clockHand].dirty == true)
        {
          //writes page back to disk
          try {
            flushFile(bufDescTable[clockHand].file);
          } catch (BadBufferException e) {
            std::cout << "File not flushed" << std::endl;
          }
        }
        
        frame = clockHand; //return by reference
        //bufDescTable[frame].Set();
        return;             //leave while loop
      }

      else
      {
        advanceClock();
        if (clockHand == originHand){
          frame = clockHand;
          return;
        }
      }
    }
  }

  void BufMgr::readPage(File &file, const PageId pageNo, Page *&page)
  {

    //check if the page is already in buffer pool by invoking the BufHashTbl::lookup method on the hashtable to get a frame number.
    //This may throw a HashNotFoundException so be ready to catch it.

    FrameId frameNo; //TODO: - Determine what starting value should be. (-1,0,1, NULL).

    try
    {
      //returns frameNo by reference
      this->hashTable.lookup(file, pageNo, frameNo);
    }
    catch (HashNotFoundException &e)
    { //Case 1: hash is not found in the hashTable

      //  -Call allocBuf() to allocate a buffer frame
      allocBuf(frameNo);

      //  -Then, call file.readPage() to read the page from disk to buffer bool frame.
      Page readPage = file.readPage(pageNo);
      this->bufPool[frameNo] = readPage;

      //should catch INVALIDPAGEEXCEPTION (#TODO: Confirm and implement catch)
      //  -Next, insert the page into the hashTable
      this->hashTable.insert(file, pageNo, frameNo);

      //  -Finally, invoke Set() on the frame to set ut up properly
      //          +Set() will automatically leave the pinCnt = 1
      this->bufDescTable[frameNo].Set(file, pageNo);
      
      //  -Return (kind of) a pointer to the frame containing the page via the page paramter
      page = &(this->bufPool[frameNo]);
      return;
    }

    // Case 2:
    // else, a HashNotFoundException was NOT caught
    // so, the Page is in the buffer pool
    // Note, the frameNo was likely changed by reference in hashTable.lookUp(...);
    // frameNo now will point to the frame to access.

    //set the appropraiate refbit
    this->bufDescTable[frameNo].refbit = true; //set to refbit to true/1
    
    //clarify the value to increment 
    const int ONE = 1;

    //increment the pinCnt for the page 
    this->bufDescTable[frameNo].pinCnt = this->bufDescTable[frameNo].pinCnt + ONE;
    

    //return a pointer to the frame containing the page via the page parameter
    // to do so, set page = the address of bufPool[#]
    page = &(this->bufPool[frameNo]);

    return;
  }

  void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty)
  {

    FrameId frameNo;

    try
    {
      this->hashTable.lookup(file, pageNo, frameNo);
    }
    catch (HashNotFoundException)
    {

      //does nothing if the page is not found in the hash table
      return;

    } //else, page was found in the hash table.

    //if dirty is true, sets the dirty bit to true
    if (dirty)
    {
      this->bufDescTable[frameNo].dirty = true;
    }

    //pinCnt logic
    if (this->bufDescTable[frameNo].pinCnt != 0)
    {
      //if the pinCnt is larger than 0, then decrement pinCnt by 1
      this->bufDescTable[frameNo].pinCnt = this->bufDescTable[frameNo].pinCnt - 1;
    }
    else
    { // else, pinCnt < 1 and it cant be decremented. throw PageNotPinnedException

      std::string nameIn = file.filename();


      //Exception format: (const std::string &nameIn, PageId pageNoIn, FrameId frameNoIn);
      throw PageNotPinnedException(nameIn, pageNo, frameNo);
    }

    return;
  }

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {

  // Allocate an empty page in the specified file
  Page allocatedPage = file.allocatePage();
  PageId allocatedPageNo = allocatedPage.page_number();

  // Obtain a buffer pool frame (id passed via FrameId variable)
  FrameId frame;

  //try {
    //std::cout << "allocBuff" << std::endl;
    allocBuf(frame);
  //} catch (BufferExceededException e) {
    //std::cout << "Buffer Full" << std::endl;
    //return;
  //}

  //std::cout << "allocBuff exit " << std::endl;

  // insert entry into hash table

  try {
    //std::cout << "insert " << std::endl;
    hashTable.insert(file, allocatedPageNo, frame);
  } catch (HashAlreadyPresentException e) {
    
    // the corresponding page already exists in the hash table
    //std::cout << e.message();
    return;
  } catch (HashTableException e) {
    
    // could not create a new bucket as running of memory
    //std::cout << e.message();
    return;
  }

  //std::cout << "exit insert " << std::endl;

  // invoke Set() on the frame
  //std::cout << frame << std::endl
  bufDescTable[frame].Set(file, allocatedPageNo);
  //std::cout << "sets " << std::endl;

  // return the page number and a pointer to the buffer frame allocated
  page = &(bufPool[frame]);
  pageNo = bufDescTable[frame].pageNo;
}

  void BufMgr::flushFile(File &file)
  {

    FrameId frameNo;

    //Scan bufTable by iterating through each frame in buffer
    for (frameNo = 0; numBufs > frameNo; frameNo++)
    {

      //get the information needed from the bufTable for the current iteration of frameNo
      BufDesc checkBuf = this->bufDescTable[frameNo];
      File checkFile = checkBuf.file;
      int checkPinCount = checkBuf.pinCnt;
      PageId checkPageNo = checkBuf.pageNo;

      //if the checkFile (from checkBuf) == file (the file parameter), then we have encountered a possible page to flush.
      if (checkFile == file)
      {

        //check PagePinnedException (if page is still pinned, throw exception)
        if (0 < checkPinCount)
        {
          //Format: PagePinnedException(const std::string &nameIn, PageId pageNoIn, FrameId frameNoIn);
          //throws PagePinnedException If any page of the file is pinned in buffer pool
          throw PagePinnedException(checkFile.filename(), checkPageNo, frameNo);
        }

        //check BadBufferException (if checkFile is invalid, throw exception)
        if (checkFile.isValid() == false)
        {
          //Format: BadBufferException(FrameId frameNoIn, bool dirtyIn, bool validIn, bool refbitIn);
          //throws BadBufferException If any frame allocated to the file is found invalid
          throw BadBufferException(frameNo, checkBuf.dirty, checkFile.isValid(), checkBuf.refbit);
        }

        //check to see if the page is dirty
        if (checkBuf.dirty == true)
        {

          //if page is dirty, call file.writePage() to flush the page to disk.
          Page pageToWrite = this->bufPool[frameNo];
          checkFile.writePage(pageToWrite);

          //then set the dirty bit for the page to false (0)
          checkBuf.dirty = false;

          //remove page from the hashTable (regardless if dirty or not)
          this->hashTable.remove(checkFile, checkPageNo);

          //invoke clear() method of BufDesc for the page frame.
          //this->bufDescTable.clear();
          this->bufDescTable[frameNo].clear();
        }
        else if (!checkBuf.dirty) //if the page is not dirty, just remove and clear.
        {

          //remove page from the hashTable (regardless if dirty or not)
          this->hashTable.remove(checkFile, checkPageNo);

          //invoke clear() method of BufDesc for the page frame.
          this->bufDescTable[frameNo].clear();
          //this->bufDescTable.clear();
        }
      }
    }

    //once we iterate through each, return
    return;
  }
  
void BufMgr::disposePage(File& file, const PageId PageNo) {
  
  // Get the frame id (id passed via FrameId variable)
  FrameId frame;
  hashTable.lookup(file, PageNo, frame);

  for (FrameId i = 0; i < bufDescTable.size(); i++) {

    // Check that page to be deleted is allocated a frame within buffer pool
    if (bufDescTable[i].frameNo == frame) {
      
      // Free the allocated frame
      bufDescTable[frame].clear();

      // Remove entry from hash table
      try {
        hashTable.remove(file, PageNo);
      } 
      catch (HashNotFoundException e) {
        std::cout << e.message();
      }
    }
  }
}


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
