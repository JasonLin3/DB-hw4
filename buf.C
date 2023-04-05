/**
Group Members:
    Jason Lin, jlin369, 9081113509
    Chris Plagge, cplagge, 9082038416

Purpose of file:
    This file implements a buffer management system to manage the pages that lie in the buffer pool at any given time.
This file implements functionalities such as reading a page into the buffer pool, allocating a new page, unpinning
pages, removing pages, and flushing the buffer pool.
*/


#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * Allocates a frame in the buffer pool. Using the clock replacement algorithm, this function
 * will find the optimal frame to replace and returns by reference which frame is allocated.
 * It then updates the frame data in the buffer table.
 * 
 * @param frame             Acts as a pass by reference variable which will be updated with the frame number allocated.
 * @return const Status     Returns a status indicating success or failed allocation.
 */
const Status BufMgr::allocBuf(int & frame) 
{
    //Find page to replace using clock
    unsigned int handStart = clockHand;
    int loops = 0;
    
    //Only run twice to check for optimal page to allocate
    while(loops < 2){
        //Handles frame not in use
        if(bufTable[clockHand].pinCnt == 0){
            //Return frame if available
            if(bufTable[clockHand].refbit == 0){
                frame = clockHand;
                break;
            }
            //Decrement refbit
            else{
                bufTable[clockHand].refbit = 0;
            }
        } 
        //Handles frame which is still in use
        advanceClock();
        loops += (clockHand == handStart) ? 1 : 0; //Will update on successful loop
    }

    //Handle loop terminating after looking at all pages
    if(loops >= 2) {
        return BUFFEREXCEEDED;
    }
    
    //Write if dirty
    if(bufTable[clockHand].dirty && bufTable[clockHand].valid){
        //Establishes parameters for write
        File* filePtr = bufTable[clockHand].file;
        int pageNo = bufTable[clockHand].pageNo;
        int frameNo;
        hashTable->lookup(filePtr, pageNo, frameNo);
        //Write to disk and handle errors
        if(filePtr->writePage(pageNo, &bufPool[frameNo]) == UNIXERR) {
            return UNIXERR;
        }
    }

    //Update hash table only if allocated frame was formerly a valid frame
    if(bufTable[clockHand].valid) {
        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
    }

    //Remove metadata for removed frame
    bufTable[clockHand].Clear();

    return OK;
}

/**
 * Reads a specific page from a file. When given the desired file and page number
 * this function will assign a pointer which was passed by reference to contain the page requested.
 * 
 * @param file              File to read page from.
 * @param PageNo            Page number within the file.
 * @param page              Pass by reference pointer to hold page once read from file.
 * @return const Status     Returns a status indicating successful or failed read.
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    
    int frameNo;
    //Finds frame number if page exists in buffer pool
    if(hashTable->lookup(file, PageNo, frameNo) ==  OK) {
        //Mark in metadata that page was requested
        bufTable[frameNo].refbit = 1;
        bufTable[frameNo].pinCnt += 1;
        //Set page variable to pass back the correct pointer
        page = &bufPool[frameNo];
        return OK;
    } 

    //Handling page not in buffer pool
    else {
        int frame;
        //Tries to make space to add page to buffer pool
        if(allocBuf(frame)==BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;
        }
        //Tries to read page from file into the buffer pool at allocated location
        if(file->readPage(PageNo, &bufPool[frame]) == UNIXERR) {
            return UNIXERR;
        } 
        else {
            //Tries to add the location of newly allocated page into the hashtable tracking the buffer pool
            if(hashTable->insert(file, PageNo, frame)==HASHTBLERROR){
                return HASHTBLERROR;
            }
            else {
                //Sets the default metadata for the new page
                bufTable[frame].Set(file, PageNo);
                //Set page variable to pass back the correct pointer to read page
                page = &bufPool[frame];
                return OK;
            }
        }

    }

}

/**
 * Unpins a page from being active. When a process finishes using a page, it can
 * call this function to decrement the pincount and thus reflect that the page is no longer in use.
 * This function will also update the page metadata to reflect if changes were made that must be 
 * written to disk upon page deallocation.
 * 
 * @param file             File on which the page to be unpinned is found
 * @param PageNo           Page number of page to be unpinned within the file
 * @param dirty            Indicates if page was edited during use
 * @return const Status    Returns status reflecting successful or failed upPinning
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    //Check if page requested is currently in the buffer pool
    int status = hashTable->lookup(file, PageNo, frameNo);
    //If page is in buffer pool
    if(status == OK) {
        //If the page is currently pinned, decrement the pin count
        if (bufTable[frameNo].pinCnt > 0) {
            //If we must mark page dirty to indicate it was edited, do so
            if(dirty) {
                bufTable[frameNo].dirty = true;
            }
            bufTable[frameNo].pinCnt -= 1;
            //If the pinCnt drops from 1 to 0, set the refbit to 1
            if(bufTable[frameNo].pinCnt == 0) {
                bufTable[frameNo].refbit = 1;
            }
            return OK;
        } 
        //Return status error if trying to unpin page that isn't pinned
        else{
            return PAGENOTPINNED;
        }
    } 
    else {
        return HASHNOTFOUND;
    }

}

/**
 * Overarching function for allocating a page in a file. This function handles interaction with the 
 * allocBuf function defined above and focuses on setting metadata for the frame 
 * which is allocated by allocBuf to reflect its addition to the file.
 * 
 * @param file              File in which to allocate a new page.
 * @param pageNo            Pass by reference variable to hold page number within the file of the new page.           
 * @param page              Pass by reference variable to hold newly allocated page pointer.
 * @return const Status     Returns status indicating success or details of failure.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    //Create new page in file
    if(file->allocatePage(pageNo)==UNIXERR) {
        return UNIXERR;
    }
    int frame;
    //Allocate frame in buffer for new page
    if(allocBuf(frame) == BUFFEREXCEEDED) {
        return BUFFEREXCEEDED;
    }
    //Insert new page into hash table
    if(hashTable->insert(file, pageNo, frame) == HASHTBLERROR) {
        return HASHTBLERROR;
    }
    //Set default metadata for new page
    bufTable[frame].Set(file, pageNo);
    file->readPage(pageNo, &bufPool[frame]);
    //Return pointer to new page in the page pass by reference variable
    page = &bufPool[frame];
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


