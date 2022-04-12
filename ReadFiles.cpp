//
// Created by 吴中奇 on 2021/11/13.
//

#include "ReadFiles.h"

ReadFiles::ReadFiles(const std::string &PATH) {
    path = PATH;
    struct dirent *ptr;
    DIR *dir;
    dir = opendir(path.c_str());
    numOfFiles = 0;
    while ((ptr = readdir(dir)) != nullptr) {
        if (ptr->d_name[0] == '.')
            continue;
        dirs.emplace_back(ptr->d_name);
        ++numOfFiles;
    }
}

BinPacking ReadFiles::getData(int index, int numThreads) {
    std::string fileName = path + '/' + dirs[index];
    int n, c;
    std::vector<int> items;

    std::ifstream infile(fileName);
    std::string line;

    getline(infile, line);
    std::istringstream iss_n(line);
    iss_n >> n;
    getline(infile, line);
    std::istringstream iss_c(line);
    iss_c >> c;

    items.reserve(n);

    while (getline(infile, line)) {
        int num;
        std::istringstream iss(line);
        iss >> num;
        items.emplace_back(num);
    }
    infile.close();
    return {c, items,numThreads};
//    return {c, items};
}

std::vector<int> ReadFiles::getVectorData(int index) {
    std::string fileName = path + '/' + dirs[index];
    int n, c;
    std::vector<int> items;

    std::ifstream infile(fileName);
    std::string line;

    getline(infile, line);
    std::istringstream iss_n(line);
    iss_n >> n;
    getline(infile, line);
    std::istringstream iss_c(line);
    iss_c >> c;

    items.reserve(n+2);
    items.emplace_back(n);
    items.emplace_back(c);
    while (getline(infile, line)) {
        int num;
        std::istringstream iss(line);
        iss >> num;
        items.emplace_back(num);
    }
    infile.close();
    return items;
}