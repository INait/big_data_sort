#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>

using namespace std;

#define MAX_MEMORY_SIZE 256 * 1024 * 1024
// #define MAX_MEMORY_SIZE 64

typedef unsigned char      UINT8;
typedef unsigned short int UINT16;
typedef unsigned long      UINT32;
typedef unsigned long long UINT64;

class FileSorter
{
private:

   // number of threads available for sorting
   UINT16 threadNum;

   // Name of the file to sort
   char* baseFileName;

   // vector with names of temporary files
   vector<string> tempFiles;

    // Arguments for divide one piece into threadNum pieces
    // And sort small pieces in parallel
    struct ArgForQSortThread
    {
        UINT16* readBuffer;
        UINT64 readBlockSize;
    };


    // Arguments to merge multithread-sorted pieces into one big piece
    struct ArgsForMergePiecesThread
    {
        vector<UINT16>* vect;
        UINT64 start;
        UINT64 end;
        int partsNum;
        int blockSize;

        ArgsForMergePiecesThread(vector<UINT16>* vect, UINT64 start, UINT64 end, int partsNum, int blockSize) :
            vect(vect), start(start), end(end), partsNum(partsNum), blockSize(blockSize) { }
    };

    // Functor for qsort algorithm
    template<class T>
    static int compare (const void * a, const void * b)
    {
        if ( *(T*)a <  *(T*)b ) return -1;
        if ( *(T*)a == *(T*)b ) return 0;
        if ( *(T*)a >  *(T*)b ) return 1;
    }

    // Get File size using linux stats
    UINT64 GetFileSize(char* fileName);

    // Sorting inside thread using quicksort algorithm
    static void* ThreadSortRoutine(void* arg);

    // Dump sorting vector
    void PrintVect(vector<UINT16>& vect, int start, int end);

    // Merging small pieces into big one inside thread
    static void* MergePieces(void* args);
    
    // Sort piece 256 mb of big file
    void SortPiece(vector<UINT16>& readBuffer, UINT64 readBlockSize);
    
    // Auxiliary function for merging vectors into file
    int FindMinFront(vector<UINT16> vect[], UINT64 readPosition[], int eofs, int filesNum);

public:

    FileSorter(char* fileName, UINT16 threadNum) : baseFileName(fileName), threadNum(threadNum) { };

    // Final stage - merge parted files into big one
    void MergeFiles();

    // Shrink file into pieces by 256 mb, sort pieces inside, then merge into big output file
    void ShrinkAndSortPieces();

    // Get time difference
    UINT64 todiff(struct timeval *tod1, struct timeval *tod2)
    {
        long long t1, t2;
        t1 = tod1->tv_sec * 1000000 + tod1->tv_usec;
        t2 = tod2->tv_sec * 1000000 + tod2->tv_usec;
        return t1 - t2;
    }
};

