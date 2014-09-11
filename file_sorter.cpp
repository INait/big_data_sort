#include "file_sorter.h"

// Get File size using linux stats
UINT64 FileSorter::GetFileSize(char* fileName)
{
    struct stat fStat;
    stat(fileName, &fStat);

    return fStat.st_size;
}

// Sorting inside thread using quicksort algorithm
void* FileSorter::ThreadSortRoutine(void* arg)
{
    ArgForQSortThread* args = (ArgForQSortThread*) arg;

    // sort content in terms of WORDS (UINT16)
    qsort(args->readBuffer, args->readBlockSize/sizeof(UINT16), sizeof(UINT16), FileSorter::compare<UINT16>);
}

void FileSorter::PrintVect(vector<UINT16>& vect, int start, int end)
{
    for (int i = start; i < end; i++)
    {
        cout << hex << vect[i] << " ";
    }
    cout << endl;
}

void* FileSorter::MergePieces(void* args)
{
    ArgsForMergePiecesThread* arg = (ArgsForMergePiecesThread*) args;

    if (arg->partsNum == 1)
    {
        return NULL;
    }
    else
    {
        int rightParts = arg->partsNum / 2;
        int leftParts = arg->partsNum - rightParts;
        
        UINT64 middle = arg->start + leftParts * arg->blockSize;

        if (arg->partsNum > 2)
        {
            pthread_t tidl, tidr;

            ArgsForMergePiecesThread* argsLeft = new ArgsForMergePiecesThread(arg->vect, arg->start, middle, leftParts, arg->blockSize);
            ArgsForMergePiecesThread* argsRight = new ArgsForMergePiecesThread(arg->vect, middle, arg->end, rightParts, arg->blockSize);

            pthread_create(&tidl, NULL, FileSorter::MergePieces, argsLeft);
            pthread_create(&tidr, NULL, FileSorter::MergePieces, argsRight);

            pthread_join(tidl, NULL);
            pthread_join(tidr, NULL);
        }

        std::inplace_merge(arg->vect->begin() + arg->start, arg->vect->begin() + middle, arg->vect->begin() + arg->end);
    }
}

// Sort piece 256 mb of big file
void FileSorter::SortPiece(vector<UINT16>& readBuffer, UINT64 readBlockSize)
{
    // to measure time
    struct timeval tod1, tod2;
    
    // create first time stamp
    gettimeofday(&tod1, NULL);
    
    // thread create
    pthread_t* tid = new pthread_t[threadNum];
    ArgForQSortThread* args = new ArgForQSortThread[threadNum];

    UINT64 blockSizePerThread = readBlockSize / threadNum;
    
    if (readBlockSize % threadNum != 0 || blockSizePerThread % sizeof(UINT16) != 0)
        blockSizePerThread += 2 - blockSizePerThread % sizeof(UINT16);

    UINT64 readBlockLeft = readBlockSize;

    for (int i = 0; i < threadNum; i++)
    {
        // initialize arguments
        args[i].readBuffer = &readBuffer[i * (blockSizePerThread/(sizeof(UINT16)))];
        
        // size is counted with last element
        args[i].readBlockSize = (readBlockLeft < blockSizePerThread) ? readBlockLeft : blockSizePerThread;

        // create thread with sorting routine
        pthread_create(&tid[i], NULL, FileSorter::ThreadSortRoutine, &args[i]);

        readBlockLeft -= args[i].readBlockSize;
    }

    // wait for sorting thread for execution
    for (int i = 0; i < threadNum; i++)
        pthread_join(tid[i], NULL);

    gettimeofday(&tod2, NULL);
    cout << "sort inside pieces ready: " << todiff(&tod2, &tod1) / 1000000.0 << " seconds" << endl;
    
    ArgsForMergePiecesThread* argsF = new ArgsForMergePiecesThread(&readBuffer, 0, readBlockSize / 2, threadNum, blockSizePerThread / 2);
    MergePieces(argsF);
    
    gettimeofday(&tod2, NULL);
    cout << "pieces merged to file: " << todiff(&tod2, &tod1) / 1000000.0 << " seconds" << endl;
}

// Auxiliary function for merging vectors into file
int FileSorter::FindMinFront(vector<UINT16> vect[], UINT64 readPosition[], int eofs, int filesNum)
{
    UINT16 minValue = (UINT16) -1;
    int minVect = 0;

    for (int i = 0; i < filesNum; i++)
    {
        if ( (((eofs >> i) & 1) != 1) 
             && ( vect[i][readPosition[i]] < minValue ) )
        {
            minVect = i;
            minValue = vect[i][readPosition[i]];
        }
    }

    return minVect;
}

// Final stage - merge parted files into big one
void FileSorter::MergeFiles()
{
    int filesNum = this->tempFiles.size();
    
    if (filesNum == 1)
    {
        stringstream ss;
        ss << "mv " << tempFiles.front() << " out.data";
        int res = system(ss.str().c_str());
        return;
    }

    int bufSize = MAX_MEMORY_SIZE / (filesNum + 1);
    if (bufSize % sizeof(UINT16) != 0)
        bufSize--;

    cout << "number of files to merge: " << filesNum << endl;
    
    // create output buffer
    vector<UINT16> outputVect;
    outputVect.reserve(bufSize / sizeof(UINT16));

    // create buffers with read counters
    // It is needed to bufferize input files
    vector<UINT16> inputVect[filesNum];
    UINT64 readPosition[filesNum];

    ifstream inFile[filesNum];      // temporary files
    unsigned int eofs = 0;          // mask of files, where end-of-files were reached
    unsigned int files = 0;         // mask of files
    UINT64 fileSizesLeft[filesNum]; // Bytes left in temporary files
    UINT64 curBlockSizes[filesNum]; // Current size of blocks which are buffered from files

    // Initialize reading of files
    for (int i = 0; i < filesNum; i++)
    {
        fileSizesLeft[i] = GetFileSize((char*)this->tempFiles.at(i).c_str());
        inputVect[i].reserve(bufSize / sizeof(UINT16));
        
        curBlockSizes[i] = ( fileSizesLeft[i] < bufSize ) ? fileSizesLeft[i] : bufSize;

        inFile[i].open(this->tempFiles.at(i).c_str(), ifstream::in | ifstream::binary);
        inFile[i].read((char*) &inputVect[i][0], curBlockSizes[i]);

        readPosition[i] = 0;

        files <<= 1;
        files++;

        fileSizesLeft[i] -= curBlockSizes[i];
    }

    ofstream outFile("out.data", ofstream::out | ostream::binary);

    // Execute merge sort on temporary bufferized files
    while ( files != eofs )
    {
        // Get minimal value from pieces array
        int minVect = FindMinFront(inputVect, readPosition, eofs, filesNum);
        
        UINT16 value = inputVect[minVect][readPosition[minVect]];

        outputVect.push_back(value);

        readPosition[minVect]++;

        // Check if we reached end of buffer
        if ( readPosition[minVect] == (curBlockSizes[minVect] / sizeof(UINT16)))
        {
            if (fileSizesLeft[minVect] == 0)
            {
                // if the file is at eof set mask and do nothing
                eofs |= (1 << minVect);
            }
            else
            {
                // if the file is not at eof read the next block into buffer
                curBlockSizes[minVect] = ( fileSizesLeft[minVect] < bufSize ) ? fileSizesLeft[minVect] : bufSize;
                fileSizesLeft[minVect] -= curBlockSizes[minVect];

                inputVect[minVect].clear();
                inFile[minVect].read((char*) &inputVect[minVect][0], curBlockSizes[minVect]);
                
                readPosition[minVect] = 0;
            }
        }

        // If output buffer is full - copy it to file and clear for next portion
        if (outputVect.size() == (bufSize / sizeof(UINT16)))
        {
            outFile.write((char*) &outputVect[0], bufSize);
            outputVect.clear();
        }
    }

    // Write the rest from output buffer to the file
    if (outputVect.size() > 0)
    {
        outFile.write((char*) &outputVect[0], 2 * outputVect.size());
        outputVect.clear();
    }

    // Close and remove temporary files
    for (int i = 0; i < filesNum; i++)
    {
        inFile[i].close();
    }

    int res = system("rm temp*");

    outFile.close();
}

void FileSorter::ShrinkAndSortPieces()
{
    // buffer with size of MAX_MEMORY_SIZE
    vector<UINT16> readBuffer(MAX_MEMORY_SIZE/sizeof(UINT16));

    // open file to sort
    ifstream inFile(baseFileName, ifstream::in | ifstream::binary);
   
    // Get its size
    UINT64 fileSize = GetFileSize(baseFileName);
    
    // counter for temp files count
    int i = 0;

    while (fileSize > 0)
    {
        stringstream ss;
        ss << "temp" << i << ".dat";

        // Add fileName to vector with temporary files
        tempFiles.push_back(ss.str());

        // if file is more than 256 MB
        UINT64 readBlockSize = fileSize > MAX_MEMORY_SIZE ? MAX_MEMORY_SIZE : fileSize;
     
        // read block of raw data
        inFile.read((char*) &readBuffer[0], readBlockSize);

        cout << i << " chunk is read, size: " << readBlockSize / 1024 / 1024 << "mb" << endl;
        
        // Sort piece of file ( <= 256 mb)
        SortPiece(readBuffer, readBlockSize);

        // write block of raw data
        ofstream outFile(ss.str().c_str(), ofstream::out | ofstream::binary);
        outFile.write((char*) &readBuffer[0], readBlockSize);
        outFile.close();

        cout << i << " chunk is sorted, size: " << readBlockSize / 1024 / 1024 << "mb" << endl;

        // reduce size of non-sorted file
        fileSize -= readBlockSize;
        i++;
    }

    inFile.close();
    
    // Free vector for the next operation
    vector<UINT16>().swap(readBuffer);
}

