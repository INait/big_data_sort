#include "file_sorter.h"

// Get number of processors in system
UINT16 GetProcNumber()
{
    FILE *in;
    char buff[512];

    if(!(in = popen("cat /proc/cpuinfo | grep processor | wc -l", "r"))){
        return 1;
    }

    int procNum = 1;

    if (fgets(buff, sizeof(buff), in) != NULL);
        sscanf(buff, "%d", &procNum);
    
    pclose(in);

    if (procNum > 16 || procNum <= 0) 
        return 1;
    else 
        return procNum;
}

// Body of program
int main(int argc, char* argv[])
{
    int procNum = 1;

    // Check if all arguments passed correctly
    if (argc < 2)
    {
        cout << "Usage: <ProgramName> file_name [num_treads]" << endl;
        return 0;
    }
    if (argc >= 3)
    {
        sscanf(argv[2], "%d", &procNum);
        cout << "number of threads is set to " << procNum << endl;
    }
    else
    {
        // Get number of threads in system
        procNum = GetProcNumber();
    
        cout << procNum << " cores available, set " << procNum << " number of threads" << endl;
    }

    // to measure time
    struct timeval tod1, tod2;
    
    // create first time stamp
    gettimeofday(&tod1, NULL);
    
    FileSorter* fileSorter = new FileSorter(argv[1], procNum);

    // shrink file into 256mb pieces
    // sort inside pieces using procNum number of threads
    fileSorter->ShrinkAndSortPieces();

    gettimeofday(&tod2, NULL);
    cout << "pieces inside sorted: " << fileSorter->todiff(&tod2, &tod1) / 1000000.0 << " seconds" << endl;

    // Merge temporary files into the big one
    fileSorter->MergeFiles();

    gettimeofday(&tod2, NULL);
    cout << "files merged: " << fileSorter->todiff(&tod2, &tod1) / 1000000.0 << " seconds" << endl;
    
    return 0;
}

