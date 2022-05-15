#include "BinPacking.h"

void printVector(const std::vector<int> &v);

BinPacking::BinPacking(int *inputData, int _numThreads) : c(inputData[1]),
                                                          weightOfItems(std::vector<int>(inputData[0])),
                                                          _UB(0), countBranches(0), numThreads(_numThreads),
                                                          solution(std::vector<int>(inputData[0], 0)),
                                                          foundRes(false), busy(0), isClosed(false) {
    int itemsSize = inputData[0];
    MPI_Comm_rank(MPI_COMM_WORLD, &id_MPI);

    weightOfItems.assign(&inputData[2], &inputData[2] + itemsSize);

}

std::vector<int> BinPacking::getSerializeInputData() {
    std::size_t itemsSize = weightOfItems.size();
    std::vector<int> list(2);
    list[0] = itemsSize;
    list[1] = c;
    list.insert(list.end(), weightOfItems.begin(), weightOfItems.end());
    return list;
}

inline std::vector<int> BinPacking::branchSerialization(Branch &branch) const {
    std::vector<int> data = std::move(branch.serialization());
    data[0] = _UB.load();
    data[1] = LB;
    return data;
}

Branch BinPacking::branchDeserialize(int *inputMessage) {
    _UB = inputMessage[0];
    int c = inputMessage[2];
    int reduced = inputMessage[3];
    int indexOfItem = inputMessage[4];
    std::vector<Item> items;
    items.reserve(inputMessage[5]);
    int index = 6;
    int end = inputMessage[5] * 2 + index;
    for (; index < end; index += 2) {
        Item item{inputMessage[index], inputMessage[index + 1]};
        items.emplace_back(std::move(item));
    }
    std::vector<int> distribution;
    distribution.reserve(inputMessage[index]);
    end = inputMessage[index] + index + 1;
    ++index;
    for (; index < end; ++index) {
        distribution.emplace_back(inputMessage[index]);
    }
    return Branch{c, reduced, indexOfItem, items, distribution};
}

inline void BinPacking::sendRequestToMaster() {
    int command[2] = {3, id_MPI};
    MPI_Send(command, 2, MPI_INT, id_root, 0, MPI_COMM_WORLD);
//    mtxRequestList.lock();
    std::unique_lock<std::mutex> locker(mtx);
    while (!requestList.empty()) {
        command[1] = requestList.top();
        requestList.pop();
        locker.unlock();
        MPI_Send(command, 2, MPI_INT, id_root, 0, MPI_COMM_WORLD);
        locker.lock();
    }
//    mtxRequestList.unlock();
}

void BinPacking::sendBetterResult() {
//    printf("worker%d: sendBetterResult,UB=%d, size=%zu\n", id_MPI, _UB.load(), solution.size());
    int command[2] = {4, (int) solution.size() + 2};
    int &sizeSend = command[1];
    MPI_Send(command, 2, MPI_INT, id_root, 0, MPI_COMM_WORLD);
    int *inputData = (int *) malloc(sizeSend * sizeof(int));
    inputData[0] = sizeSend - 2;
    inputData[1] = _UB.load();
    std::copy(solution.begin(), solution.end(), &inputData[2]);
    MPI_Send(inputData, sizeSend, MPI_INT, id_root, 1, MPI_COMM_WORLD);

    free(inputData);
}

void BinPacking::updateUB(int UB) {
    if (_UB > 0) {
        while (UB < _UB)
            _UB = UB;
    }
}

int BinPacking::recvBetterResult(int size) {
    int *solutionArray = (int *) malloc(size * sizeof(int));
    MPI_Recv(solutionArray, size, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    int sizeOfSolution = solutionArray[0];
    int UB_current = solutionArray[1];
    std::vector<int> newSolution(solutionArray + 2, solutionArray + size);
    free(solutionArray);
    if (UB_current == LB) foundRes.store(true);
    int UB = _UB.load();
    bool changed = false;
    while (UB > UB_current) {
        changed = true;
        solution = std::move(newSolution);
        if (_UB.compare_exchange_weak(UB, UB_current)) {
            UB = _UB.load();
            break;
        }
    }

//    printSolution1();
    if (changed) {
        return UB_current;
    } else {
        return 0;
    }
}

Branch BinPacking::init() {
    std::vector<Item> items = refactor(weightOfItems);
    sort(items.rbegin(), items.rend());
    Branch branch{c, items};

    branch.reduction();
    solution = branch.getDistribution();
    LB = branch.lowerBound2();
    _UB.store(branch.upperBound(solution));


    if (_UB == LB) {
        organize();
        foundRes.store(true);
    }
    LB = branch.lowerBound3();
    if (_UB == LB) {
        organize();
        foundRes.store(true);
    }

//    bfs(std::move(branch));
//    threadPool->waitForFinished();
//    organize();
    return branch;
}

void BinPacking::BNB(Branch branch) {
    initThreadPool();
    isClosed = false;
    append(std::move(branch));
    waitForFinished();
    endThreadPool();

    sendRequestToMaster();
//    printf("worker%d: rest\n", id_MPI);
}

void BinPacking::bfs(Branch branch) {

    ++countBranches;
    //    printf("%d\n",countBranches.load());
    int z = branch.getIndexOfItem();
    std::vector<Item> &items = branch.getItems();

    if (z == items.size() || items[z].weight == 0) return;

    //to all feasible initialized bins
    int j;
    z = branch.getIndexOfItem();
    for (j = z - 1; j >= 0 && !foundRes; --j) {
        if (items[j].weight + items[z].weight <= c) {
            Branch newBranch(branch);
            newBranch.mergeTwoItems(j, z);
            newBranch.reduction();
            std::vector<int> curSolution(newBranch.getDistribution());
            int LB_current = newBranch.lowerBound2();
            int UB_current = newBranch.upperBound(curSolution);

            if (UB_current == LB) {
                solution = std::move(curSolution);
                _UB.store(UB_current);
                foundRes.store(true);
                isClosed.store(true);
                clearQueue();
                sendBetterResult();
                return;
            }
            int UB = _UB.load();
            while (UB > UB_current) {
                solution = std::move(curSolution);
                if (_UB.compare_exchange_weak(UB, UB_current)) {
                    UB = _UB.load();
                    sendBetterResult();
                    break;
                }
            }

            if (UB_current > LB_current && LB_current < UB) {
                if (newBranch.getIndexOfItem() + newBranch.getReduced() < UB) {
                    if (newBranch.lowerBound3() < UB) {
                        append(std::move(newBranch));
                    }
                }
            }

        }
    }

    //create a new bin
    if (z + branch.getReduced() < _UB && !foundRes) {
        Branch newBranch(branch);
        //        newBranch->addCurrentItem();
        newBranch.incrementIndex();
        newBranch.reduction();

        std::vector<int> curSolution(newBranch.getDistribution());
        int LB_current = newBranch.lowerBound2();
        int UB_current = newBranch.upperBound(curSolution);

        if (UB_current == LB) {
            solution = std::move(curSolution);
            _UB.store(UB_current);
            foundRes.store(true);
            isClosed.store(true);
            clearQueue();
            sendBetterResult();
            return;
        }
        int UB = _UB.load();
        while (UB > UB_current) {
            solution = std::move(curSolution);
            if (_UB.compare_exchange_weak(UB, UB_current)) {
                UB = _UB.load();
                sendBetterResult();
                break;
            }
        }
        if (UB_current > LB_current && LB_current < UB) {

            if (newBranch.getIndexOfItem() + newBranch.getReduced() < UB) {
                if (newBranch.lowerBound3() < UB) {
                    append(std::move(newBranch));
                }
            }
        }

    }
    //    delete branch;
}


void BinPacking::organize() {
    int min = *std::min_element(solution.begin(), solution.end());
    if (min < 0) min = -min;
    else return;
    for (int &elem: solution) {
        if (elem < 0)elem += min + 1;
        else if (elem > 0) elem += min;
    }
}

inline void printVector(const std::vector<int> &v) {
    for (const int index: v) {
        printf("%d ", index);
    }
    printf("\n");
}

void BinPacking::printWeightItems() {
    printVector(weightOfItems);
}

void BinPacking::printSolution1() {
    printVector(solution);
}

void BinPacking::printSolution2() {
    std::vector<std::vector<int>> res(_UB.load());
    for (int i = 0; i < solution.size(); ++i) {
        res[solution[i] - 1].push_back(i);
    }
    for (auto &bin: res) {
        printf("{");
        for (int item: bin) {
            printf("%d,", item);
        }
        printf("},");
    }
    printf("\n");
}

void BinPacking::respondRequests() {
    int command[2];
    std::unique_lock<std::mutex> locker1(mtx);
    std::unique_lock<std::mutex> locker2(mtxRequestList);
    while (workQueue.size() > 5 && !requestList.empty()) {
        int dest = std::move(requestList.top());
        requestList.pop();
        locker2.unlock();
        Branch task = std::move(workQueue.top());
        workQueue.pop();
        locker1.unlock();

        std::vector<int> sendData = branchSerialization(task);
        command[0] = 2;//branch data
        command[1] = sendData.size();//size of sendData
        MPI_Send(command, 2, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(sendData.data(), command[1], MPI_INT, dest, 2, MPI_COMM_WORLD);
//        printf("worker%d: send branch to worker%d\n", id_MPI, dest);
        locker1.lock();
        locker2.lock();
    }
}

void BinPacking::readRequest(int command[2]) {
    requestList.emplace(command[1]);
//    printf("read request from node%d\n", command[1]);
    respondRequests();
}

void BinPacking::append(Branch &&task) {
    if (foundRes) return;
    mtx.lock();
    workQueue.emplace(std::forward<Branch>(task));
    mtx.unlock();
    cond.notify_one();
    if (workQueue.size() > 5 && !requestList.empty()) {
        respondRequests();
    }
//    printf("node%d: append/workQueue.size()=%lu,UB=%d\n", id_MPI, workQueue.size(), _UB.load());
}

void BinPacking::appendInitBranch(Branch &&task) {
    if (foundRes) return;
    mtx.lock();
    workQueue.emplace(std::forward<Branch>(task));
    working = true;
    mtx.unlock();
    cond.notify_one();
    if (workQueue.size() > 5 && !requestList.empty()) {
        respondRequests();
    }
//    printf("node%d: append/workQueue.size()=%lu,UB=%d\n", id_MPI, workQueue.size(), _UB.load());
}

void BinPacking::waitForFinished() {
    std::unique_lock<std::mutex> lock(mtx);
    while (!isClosed) {
        finished.wait(lock, [this]() {
//            printf("node%d,size=%lu,busy=%d,working=%d\n", id_MPI, workQueue.size(), busy, working);
            return workQueue.empty() && (busy == 0) && working;
        });
        working = false;
        lock.unlock();
//        printf("worker%d: send request\n", id_MPI);
        sendRequestToMaster();
        lock.lock();
    }
}

void BinPacking::worker() {
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
        } else if (isClosed) {
            finished.notify_one();
            break;
        } else {
            finished.notify_one();
            cond.wait(locker);
        }
    }
}