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


const Status BufMgr::allocBuf(int & frame) 
{
    //Find page to replace using clock
    unsigned int handStart = clockHand;
    int loops = 0;
    while(loops < 2){
        if(bufTable[clockHand].pinCnt == 0){
            //Return page
            if(bufTable[clockHand].refbit == 0){
                frame = clockHand;
                break;
            }
            //Decrement refbit
            else{
                bufTable[clockHand].refbit = 0;
            }
        } 
        //Update
        advanceClock();
        loops += (clockHand == handStart) ? 1 : 0;
    }

    //Handle loop terminating after looking at all pages
    if(loops >= 2) {
        return BUFFEREXCEEDED;
    }
    
    //Write if dirty
    if(bufTable[clockHand].dirty && bufTable[clockHand].valid){
        File* filePtr = bufTable[clockHand].file;
        int pageNo = bufTable[clockHand].pageNo;
        int frameNo;
        hashTable->lookup(filePtr, pageNo, frameNo);
        if(filePtr->writePage(pageNo, &bufPool[frameNo]) == UNIXERR) {
            return UNIXERR;
        }
    }

    //Update hash table
    if(bufTable[clockHand].valid) {
        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
    }
    bufTable[clockHand].Clear();

    return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    // page in buffer pool - case 2
    if(hashTable->lookup(file, PageNo, frameNo) ==  OK) {
        bufTable[frameNo].refbit = 1;
        bufTable[frameNo].pinCnt += 1;
        page = &bufPool[frameNo];
        return OK;
    } 
    // page not in buffer pool - case 1
    else {
        int frame;
        if(allocBuf(frame)==BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;
        }
        // Page* newPage = new Page();
        if(file->readPage(PageNo, &bufPool[frame]) == UNIXERR) {
            return UNIXERR;
        } else {
            if(hashTable->insert(file, PageNo, frame)==HASHTBLERROR){
                return HASHTBLERROR;
            }
            else {
                bufTable[frame].Set(file, PageNo);
                page = &bufPool[frame];
                return OK;
            }
        }

    }

}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    int status = hashTable->lookup(file, PageNo, frameNo);
    if(status == OK) {
        if (bufTable[frameNo].pinCnt > 0) {
            if(dirty) {
                bufTable[frameNo].dirty = true;
            }
            bufTable[frameNo].pinCnt -= 1;
            if(bufTable[frameNo].pinCnt == 0) {
                bufTable[frameNo].refbit = 1;
            }
            return OK;
        } else{
            return PAGENOTPINNED;
        }
    } else {
        return HASHNOTFOUND;
    }

}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    if(file->allocatePage(pageNo)==UNIXERR) {
        return UNIXERR;
    }
    int frame;
    if(allocBuf(frame) == BUFFEREXCEEDED) {
        return BUFFEREXCEEDED;
    }
    if(hashTable->insert(file, pageNo, frame) == HASHTBLERROR) {
        return HASHTBLERROR;
    }

    bufTable[frame].Set(file, pageNo);
    file->readPage(pageNo, &bufPool[frame]);
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


