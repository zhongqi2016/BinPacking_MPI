#include <iostream>
#include <unordered_set>
#include "ReadFiles.h"

using namespace std;


void otherNodes(int numThreads) {
    bool run;
    bool stop = false;
    int procId, numProcs;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    MPI_Comm_rank(MPI_COMM_WORLD, &procId);
    while (!stop) {
        int command[2];
//        MPI_Bcast(command, 2, MPI_INT, id_root, MPI_COMM_WORLD);
//        int *inputData = (int *) malloc(command[1] * sizeof(int));
//        MPI_Bcast(inputData, command[1], MPI_INT, id_root, MPI_COMM_WORLD);
        std::unique_ptr<BinPacking> binPacking;
//        BinPacking binPacking = BinPacking::dataDeserialize(inputData, numThreads);
//        free(inputData);
        run = true;
        //0: not run, 1:inputData, 2:branch data, 3:request for data, 4:a better solution, 5:got data
        while (run) {
            MPI_Recv(command, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("node%d: got command %d\n", procId, command[0]);
            switch (command[0]) {
                case -1: {
                    stop = true;
                    break;
                }
                case 0: {
                    run = false;
                    break;
                }
                case 1: {
                    int *inputData = (int *) malloc(command[1] * sizeof(int));
                    MPI_Recv(inputData, command[1], MPI_INT, id_root, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    binPacking.reset(new BinPacking(inputData, numThreads));
                    free(inputData);
                    printf("node%d: got bin packing data size=%d\n", procId, command[1]);
                    break;
                }
                case 2: {
                    printf("get branch size=%d\n", command[1]);
                    int *inputData = (int *) malloc(command[1] * sizeof(int));
                    MPI_Recv(inputData, command[1], MPI_INT, id_root, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    Branch branch = binPacking->branchDeserialize(inputData);
                    free(inputData);
                    printf("node%d: got branch data\n", procId);
                    command[0] = 5;
                    command[1] = procId;
                    MPI_Send(command, 2, MPI_INT, id_root, 0, MPI_COMM_WORLD);
                    binPacking->BNB(branch);
                    break;
                }
                case 4: {
                    binPacking->updateUB(command[1]);
//                    binPacking->recvBetterResult(command[1]);
                    break;
                }
            }
        }
        binPacking.reset();
    }
}

void masterNode(string &path, int num_threads) {
    ReadFiles readFiles(path);
    readFiles.sortDirs();
    int numProcs;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    for (int i = 0; i < readFiles.getNumOfFiles(); ++i) {
        cout << i << ". " << readFiles.getFileName(i) << endl;

        BinPacking binPacking = readFiles.getData(i, num_threads);
        std::vector<int> inputData = binPacking.getSerializeInputData();

        Branch branch = binPacking.init();
        if (binPacking.resFound()) {
            //Got result
            int result = binPacking.getUB();
            int countBranches = binPacking.getCountBranches();
            binPacking.printSolution2();
            printf("result=%d, time=, branches=%d\n", result, countBranches);
        } else {
            int command[2]{1, (int) inputData.size()};
            for (int idOfOtherProcs = 1; idOfOtherProcs < numProcs; ++idOfOtherProcs) {
                MPI_Send(command, 2, MPI_INT, idOfOtherProcs, 0, MPI_COMM_WORLD);
                MPI_Send(inputData.data(), command[1], MPI_INT, idOfOtherProcs, 1, MPI_COMM_WORLD);
                printf("send binPacking to %d,size=%d\n", idOfOtherProcs, command[1]);
            }
//            MPI_Bcast(command, 2, MPI_INT, id_root, MPI_COMM_WORLD);
//            MPI_Bcast(inputData.data(), command[1], MPI_INT, id_root, MPI_COMM_WORLD);

            //send branch to one of node
            std::vector<int> serialBranch = binPacking.branchSerialization(branch);
            command[0] = 2;
            command[1] = (int) serialBranch.size();
            printf("send branch,size=%d\n", command[1]);
            MPI_Send(command, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
            MPI_Send(&serialBranch.front(), command[1], MPI_INT, 1, 2, MPI_COMM_WORLD);
            bool run = true;
            std::unordered_set<int> workingList;
            workingList.reserve(numProcs);
            workingList.emplace(1);
            for (int j = 2; j < numProcs; ++j) {
                command[0] = 3;
                command[1] = j;
                MPI_Send(command, 2, MPI_INT, 1, 0, MPI_COMM_WORLD);
                printf("master: sent request to node1\n");
            }
            while (run) {
                //command--- 0: not run, 1:inputData, 2:branch data, 3:request for data, 4:a better solution, 5:got data
                MPI_Recv(command, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                switch (command[0]) {
                    case 0: {
                        //have got the best solution,stop working
                        run = false;
                        for (int j = 1; j < numProcs; ++j) {
                            MPI_Send(command, 2, MPI_INT, j, 0, MPI_COMM_WORLD);
                        }
                        break;
                    }
                    case 3: {
                        //got a request;
                        auto notWorkingNode = workingList.find(command[1]);
                        if (notWorkingNode != workingList.end()) {
                            workingList.erase(notWorkingNode);
                        }
                        printf("master: got request from %d\n", command[1]);
                        auto workingNode = workingList.begin();
                        if (workingNode == workingList.end()) {
                            run = false;
                            break;
                        }
                        MPI_Send(command, 2, MPI_INT, *workingNode, 0, MPI_COMM_WORLD);
                        printf("master: sent request to %d\n", *workingNode);
                        break;
                    }
                    case 4: {
                        int betterUB = binPacking.recvBetterResult(command[1]);
                        command[1] = betterUB;
                        printf("master: got a better upper bound:%d\n", betterUB);
                        for (int j = 1; j < numProcs; ++j) {
                            MPI_Send(command, 2, MPI_INT, j, 0, MPI_COMM_WORLD);
                        }
                        break;
                    }
                    case 5: {
                        //node got data
                        workingList.emplace(command[1]);
                        break;
                    }
                }
                printf("master: size of workinglist %d\n", workingList.size());
                if (workingList.empty()) {
                    run = false;
                }
            }
            binPacking.organize();
            binPacking.printSolution2();
            int result = binPacking.getUB();
            int countBranches = binPacking.getCountBranches();
            printf("result=%d, time=, branches=%d\n", result, countBranches);
        }
    }
}

int main(int argc, char *argv[]) {
    const int num_threads = 4; //Set number of threads
    string path = "./bin1data/"; //Path of data

    int procId, numProcs, provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        printf("The threading support level is lesser than that demanded.\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    } else {
        printf("The threading support level corresponds to that demanded.\n");
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    MPI_Comm_rank(MPI_COMM_WORLD, &procId);
    if (procId == 0) {
        masterNode(path, num_threads);
    } else {
        otherNodes(num_threads);
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
