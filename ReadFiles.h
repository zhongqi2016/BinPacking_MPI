//
// Created by 吴中奇 on 2021/11/13.
//

#ifndef BINPACKING_BNB_READFILES_H
#define BINPACKING_BNB_READFILES_H

#include <string>
#include "BinPacking.h"

class ReadFiles {
public:
    explicit ReadFiles(const std::string &PATH);

    BinPacking getData(int index, int numThreads);

    std::vector<int> getVectorData(int index);

    std::string &getFileName(int index) { return dirs[index]; }

    int getNumOfFiles() const { return numOfFiles; }

    void sortDirs() { std::sort(dirs.begin(), dirs.end()); }

private:
    std::string path;
    int numOfFiles;
    std::vector<std::string> dirs;
};


#endif //BINPACKING_BNB_READFILES_H
