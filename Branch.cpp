//
// Created by 吴中奇 on 2021/11/1.
//
#include <fstream>
#include <sstream>
#include "Branch.h"

std::vector<int> Branch::serialization() {
    std::vector<int> message(2); //(UB+LB)
    ///capacity+reduced+indexOfItem+items+distribution
    message.reserve(7 + 2 * items.size() + distribution.size());
    message.emplace_back(c);
    message.emplace_back(reduced);
    message.emplace_back(indexOfItem);
    message.emplace_back(items.size());
    for (Item &item:items) {
        message.emplace_back(item.index);
        message.emplace_back(item.weight);
    }
    message.emplace_back(distribution.size());
    for (int dis:distribution) {
        message.emplace_back(dis);
    }
    return message;
}

inline int Branch::L2withA(int alpha) const {
    int J1 = 0,J2 = 0,J3 = 0;
    int sumJ3 = 0,sumJ2 = 0;
    int length = int(items.size());
    for (int i = 0; i < length; i++) {
        int weight_i = items[i].weight;
        if (weight_i != 0) {
            if (weight_i > c - alpha) {
                J1++;
            } else if (weight_i > c / 2) {
                J2++;
                sumJ2 += weight_i;
            } else if (weight_i >= alpha) {
                J3++;
                sumJ3 += weight_i;
            }
        }
    }
    int L = J1 + J2 + (std::max<int>(0, sumJ3 - (J2 * c - sumJ2)) + c - 1) / c;
    return L;
}

int Branch::lowerBound2() const {
    int max = 0;
    int L;
    for (int a = 0; a <= c / 2; a++) {
        L = L2withA(a);
        if (L > max) {
            max = L;
        }
    }
    return max + reduced;
}

void setDistribution(std::vector<int> &distribution, int num, int index) {
    while (distribution[index] > 0) {
        int next_index = distribution[index];
        distribution[index] = num;
        index = next_index;
    }
    distribution[index] = num;
}

int Branch::upperBound(std::vector<int> &currSolution) {
    int result = 0;
    int length = items.size();
    int freeSpace[length];

    for (int i = 0; i < length; ++i) {
        if (items[i].weight == 0) continue;
        int j;
        int min = c + 1;
        int bin_min = 0;
        int weight_i = items[i].weight;
        for (j = 0; j < result; j++) {
            if (freeSpace[j] >= weight_i && freeSpace[j] - weight_i < min) {
                min = freeSpace[j] - weight_i;
                bin_min = j;
            }
        }
        if (min == c + 1) {
            freeSpace[result] = c - weight_i;
            setDistribution(currSolution, result + 1, items[i].index);
            ++result;
        } else {
            freeSpace[bin_min] -= weight_i;
            setDistribution(currSolution, bin_min + 1, items[i].index);
        }
    }
    return result + reduced;
}

//item1 <- item2
void Branch::mergeTwoItems(int index1, int index2) {
    items[index1].weight += items[index2].weight;
    items[index2].weight = 0;
    int i = items[index1].index;
    while (distribution[i] != 0) {
        i = distribution[i];
    }
    distribution[i] = items[index2].index;
}

//void Branch::addCurrentItem() {
//    items[indexOfItem].combination = true;
//
//}

int Branch::findLargestK(int index) const {
    int sum = items[index].weight;
    int k = 0;

    for (int i = int(items.size()) - 1; i > 0; --i) {
        int weight_i = items[i].weight;
        if (weight_i == 0 || i == index) continue;
        if (sum + weight_i <= c) {
            sum += weight_i;
            ++k;
        } else {
            break;
        }
    }
    return k;
}

int Branch::findMinH(int index) const {
    int weight_i = items[index].weight;
    for (int i = 0; i < items.size(); ++i) {
        if (items[i].weight == 0 || i == index) continue;
        if (weight_i + items[i].weight <= c) {
            return i;
        }
    }
    //fail
    return -1;
}

bool Branch::findAB(int index, int &a, int &b) const {
    int weightCanUse = c - items[index].weight;
    int i;
    int maxValue = 0;
    bool flag = false;
    for (i = 0; i < items.size(); ++i) {
        int weight_i = items[i].weight;
        if (weight_i == 0 || i == index) continue;
        if (weightCanUse > weight_i) {
            for (int j = i + 1; j < items.size(); ++j) {
                int weight_j = items[j].weight;
                if (weight_j == 0 || j == index) continue;
                if (weight_i + weight_j == weightCanUse) {
                    a = i;
                    b = j;
                    return true;
                }
                if (weight_i + weight_j < weightCanUse) {
                    if (weight_i + weight_j > maxValue) {
                        maxValue = weight_i + weight_j;
                        a = i;
                        b = j;
                        flag = true;
                    }
                    break;
                }
            }
        }
    }
    return flag;
}

void Branch::reduction() {
    std::vector<int> bj(items.size(), 0);
    std::vector<int> F;
    int count = 0;//Deleted items that have been put in bins
    for (int i = 0; i < items.size(); ++i) {
        if (items[i].weight == 0) continue;
        int k = findLargestK(i);
        F.clear();
        if (k == 0) {
            F.emplace_back(i);
        } else {
            int h = findMinH(i);
            if (h == -1) continue;
            if (k == 1 || items[i].weight + items[h].weight == c) {
                F.emplace_back(i);
                F.emplace_back(h);
            } else if (k == 2) {
                int a;
                int b;
                if (findAB(i, a, b)) {
                    if (items[h].weight >= items[a].weight + items[b].weight) {
                        F.emplace_back(i);
                        F.emplace_back(h);
                    } else if (items[h].weight == items[a].weight &&
                               (b - a <= 2 || items[i].weight + items[b - 1].weight + items[b - 2].weight > c)) {
                        F.emplace_back(i);
                        F.emplace_back(a);
                        F.emplace_back(b);
                    }
                }
            }
        }
        if (!F.empty()) {
            ++reduced;
            for (int j : F) {
                setDistribution(distribution, -reduced, items[j].index);
                items[j].weight = 0;
                if (j < indexOfItem) {
                    ++count;
                }
            }
//            printDistribution();
        }
    }
    indexOfItem -= count;
    sort(items.rbegin(), items.rend());
//    if (items[items.size() - reduced] == 0) {
//        items.resize(items.size() - reduced);
//    }

}


int L2withA(int alpha, const std::vector<int> &w, int c) {
    int J1 = 0;
    int J2 = 0;
    int J3 = 0;
    int sumJ3 = 0;
    int sumJ2 = 0;
    int length = int(w.size());
    for (int i = 0; i < length; i++) {
        if (w[i] != 0) {
            if (w[i] > c - alpha) {
                J1++;
            } else if (w[i] > c / 2) {
                J2++;
                sumJ2 += w[i];
            } else if (w[i] >= alpha) {
                J3++;
                sumJ3 += w[i];
            }
        }
    }

    int L = J1 + J2 + (std::max<int>(0, sumJ3 - (J2 * c - sumJ2)) + c - 1) / c;
    return L;
}

int lowerBound2ForL3(const std::vector<int> &w1, int c) {
    int max = 0;
    int L;

    for (int a = 0; a <= c / 2; a++) {
        L = L2withA(a, w1, c);
        if (L > max) {
            max = L;
        }
    }
    return max;
}


int findLargestK(const std::vector<int> &w, int index, int c) {
    int sum = w[index];
    int k = 0;

    for (int i = int(w.size()) - 1; i > 0; --i) {
        if (w[i] == 0 || i == index) continue;
        if (sum + w[i] <= c) {
            sum += w[i];
            ++k;
        } else {
            break;
        }
    }
    return k;
}

int findMinH(const std::vector<int> &w, int index, int c) {
    int wj = w[index];
    for (int i = 0; i < w.size(); ++i) {
        if (w[i] == 0 || i == index) continue;
        if (wj + w[i] <= c) {
            return i;
        }
    }
    //fail
    return -1;
}


bool findAB(const std::vector<int> &w, int index, int &a, int &b, int c) {
    int ava = c - w[index];
    int left;
    int maxValue = 0;
    bool flag = false;
    int right = 1;
    for (left = 0; left < w.size(); ++left) {
        if (w[left] == 0 || left == index) continue;
        if (ava > w[left]) {
            for (right = left + 1; right < w.size(); ++right) {
                if (w[right] == 0 || right == index) continue;
                if (w[left] + w[right] == ava) {
                    a = left;
                    b = right;
                    return true;
                }
                if (w[left] + w[right] < ava) {
                    if (w[left] + w[right] > maxValue) {
                        maxValue = w[left] + w[right];
                        a = left;
                        b = right;
                        flag = true;
                    }
                    break;
                }
            }
        }
    }
    return flag;
}

int reductionForL3(std::vector<int> &w, int c) {
    int zr = 0;
    std::vector<int> F;
    int reduced = 0;
    for (int i = 0; i < w.size(); ++i) {
        if (w[i] == 0) continue;
        int k = findLargestK(w, i, c);
        F.clear();
        if (k == 0) {
            F.emplace_back(i);
        } else {
            int h = findMinH(w, i, c);
            if (h == -1) continue;
            if (k == 1 || w[i] + w[h] == c) {
                F.emplace_back(i);
                F.emplace_back(h);
            } else if (k == 2) {
                int a;
                int b;
                if (findAB(w, i, a, b, c)) {
                    if (w[h] >= w[a] + w[b]) {
                        F.emplace_back(i);
                        F.emplace_back(h);
                    } else if (w[h] == w[a] && (b - a <= 2 || w[i] + w[b - 1] + w[b - 2] > c)) {
                        F.emplace_back(i);
                        F.emplace_back(a);
                        F.emplace_back(b);
                    }
                }
            }
        }
        if (!F.empty()) {
            ++zr;
            for (int j : F) {
                w[j] = 0;
                reduced++;
            }

        }

    }
    sort(w.rbegin(), w.rend());
    if (w[w.size() - reduced] == 0) {
        w.resize(w.size() - reduced);
    }
    return zr;
}


int Branch::lowerBound3() const {
    int n = items.size();
    std::vector<int> w(n);
    for (int i = 0; i < n; ++i) {
        w[i] = items[i].weight;
    }

    int L3 = 0;
    int L2;
    int index = 0;

    while (n >= 1) {
        index += reductionForL3(w, c);
        n = w.size();
        if (n == 0) L2 = 0;
        else L2 = lowerBound2ForL3(w, c);
        L3 = std::max(L3, index + L2);
        if (n > 0) {
            n = n - 1;
            w.pop_back();
        }
    }

    return L3 + reduced;
}


void Branch::printDistribution() const {
    for (auto index:distribution) {
        printf("%d ", index);
    }
    printf("\n");
}
