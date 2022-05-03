
#ifndef BINPACKING_BNB_ALGORITHM_H
#define BINPACKING_BNB_ALGORITHM_H

#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <dirent.h>
#include <stack>
#include <omp.h>
#include <mpi.h>
#include <atomic>
#include <cstring>

#include <thread>
#include <vector>
#include <queue>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "Branch.h"

#define id_root 0

class BinPacking {
public:
    std::atomic<bool> working;

    BinPacking(int _c, std::vector<int> &_weight, int _numThreads) : c(_c), weightOfItems(std::move(_weight)),
                                                                     _UB(0), countBranches(0), numThreads(_numThreads),
                                                                     solution(std::vector<int>(_weight.size(), 0)),
                                                                     foundRes(false), busy(0), isClosed(false) {
        MPI_Comm_rank(MPI_COMM_WORLD, &id_MPI);
    }

    BinPacking(int *inputData, int _numThreads);

    Branch init();

    void BNB(Branch branch);

    void bfs(Branch branch);

//    static BinPacking dataDeserialize(int *inputData, int numThreads);

    std::vector<int> getSerializeInputData();

    std::vector<int> branchSerialization(Branch &branch) const;

    Branch branchDeserialize(int *inputMessage);

    void sendRequestToMaster();

    void sendBetterResult();

    int recvBetterResult(int size);

    void updateUB(int UB);

    void organize();

    std::vector<int> &getSolution() { return solution; }

    //solution: 1,1,2,2,3,3,
    //the number is serial of bin where each item is located
    void printSolution1();

    //solution: {1,2,},{3,4,},{5,6,}
    //the number above is the number of each item. Items in a bracket will be placed in the same box.
    void printSolution2();

    void printWeightItems();

    int getCountBranches() {
        return countBranches;
    }

    bool resFound() { return foundRes.load(); }

    int getUB() { return _UB.load(); }

    int getLB() { return LB; }

    void readRequest(int command[2]);

    void recvCommandStop() {
        printf("recvcmdstop\n");
        foundRes.store(true);
        clearQueue();
        endThreadPool();
    }

    void initThreadPool() {
        for (int i = 0; i < numThreads; ++i) {
            std::thread(&BinPacking::worker, this).detach();
        }
    }

    void append(Branch &&task) {
        if (foundRes) return;
        //        std::lock_guard<std::mutex> locker(mtx);
        mtx.lock();
        workQueue.emplace(std::forward<Branch>(task));
        mtx.unlock();
        cond.notify_one();
        if (workQueue.size() > 10 && !requestList.empty()) {
            respondRequests();
        }
        printf("node%d: append/workQueue.size()=%lu,UB=%d\n", id_MPI, workQueue.size(), _UB.load());
    }


    void waitForFinished() {
        std::unique_lock<std::mutex> lock(mtx);
        while (!isClosed) {
            finished.wait(lock, [this]() {
                printf("node%d,size=%d,busy=%d,working=%d\n",id_MPI,workQueue.size(),busy,working.load());
                return workQueue.empty() && (busy == 0)&&working;
            });
            working= false;
            lock.unlock();
            sendRequestToMaster();
            lock.lock();
        }
    }

private:
    //----ThreadPool---

    void respondRequests();


    void worker() {
        std::unique_lock<std::mutex> locker(mtx);
        while (true) {
            if (!workQueue.empty()) {
                ++busy;
                Branch task = std::move(workQueue.top());
                workQueue.pop();
                locker.unlock();
                bfs(std::move(task));
                locker.lock();
                --busy;
                finished.notify_one();
            } else if (isClosed) {
                break;
            } else {

                cond.wait(locker);
            }
        }
    }


    /*
    void append(Branch &&task) {
        MPI_Request request;
        int command[2];
        int flag = -1;
        if (flag != 0) {
            MPI_Irecv(&command, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &request);
            flag = 0;
        }
        MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
        if (flag) {
            printf("recved\n");
            printf("node%d, recv command%d\n", id_MPI, command[0]);
            if (command[0] == 4) {
                printf("node%d: got a better UB=%d\n", id_MPI, command[1]);
                while (command[1] < _UB) {
                    _UB.store(command[1]);
                }
            } else if (command[0] == 3) {
                if (workQueue.size() > 10) {
                    int dest = command[1];
                    std::vector<int> sendData = branchSerialization(task);
                    command[0] = 2;//branch data
                    command[1] = sendData.size();//size of sendData
                    MPI_Send(command, 2, MPI_INT, dest, 0, MPI_COMM_WORLD);
                    MPI_Send(sendData.data(), command[1], MPI_INT, dest, 2, MPI_COMM_WORLD);
                    return;
                } else {
                    mtxRequestList.lock();
                    requestList.emplace(command[1]);
                    mtxRequestList.unlock();
                }
            } else if (command[0] == 0) {
                printf("recvcmdstop\n");
                foundRes.store(true);
                isClosed.store(true);
                clearQueue();
            }
            flag = -1;
        }

        while (!requestList.empty() && workQueue.size() > 10) {
            int dest = -1;
            mtxRequestList.lock();
            dest = requestList.top();
            requestList.pop();
            mtxRequestList.unlock();
            if (dest != -1) {
                std::vector<int> sendData = branchSerialization(task);
                command[0] = 2;//branch data
                command[1] = sendData.size();//size of sendData
                MPI_Send(command, 2, MPI_INT, dest, 0, MPI_COMM_WORLD);
                MPI_Send(sendData.data(), command[1], MPI_INT, dest, 2, MPI_COMM_WORLD);
                return;
            }
        }
        //
        std::lock_guard<std::mutex> locker(mtx);
        workQueue.emplace(std::forward<Branch>(task));
        printf("node%d: append/workQueue.size()=%lu,UB=%d\n", id_MPI, workQueue.size(), _UB.load());
        cond.notify_one();

    }
*/


    void clearQueue() {
        std::lock_guard<std::mutex> locker(mtx);
        std::stack<Branch> empty;
        swap(empty, workQueue);
    }

    void endThreadPool() {
        isClosed = true;

        cond.notify_all();
    }

private:

    int id_MPI;
    //pool
    std::mutex mtx;
    std::atomic<bool> isClosed;
    std::stack<Branch> workQueue;
    std::condition_variable cond;
    std::condition_variable finished;
    unsigned int busy;

    //
    int numThreads;

    int c;//capacity of bin
    std::atomic<int> _UB;
    std::atomic<int> countBranches;
//    std::atomic<int> nonWorkingThreads;
//    std::atomic<int> numberOfTasks;
    int LB{};
    std::atomic<bool> foundRes;
    std::vector<int> weightOfItems;
    std::vector<int> solution;//Current optimal solution

    std::mutex mtxRequestList;
    std::stack<int> requestList;
};


#endif //BINPACKING_BNB_ALGORITHM_H

