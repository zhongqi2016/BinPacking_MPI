//
// Created by 吴中奇 on 2021/11/1.
//

#ifndef BINPACKING_BNB_BOUND_H
#define BINPACKING_BNB_BOUND_H

#include <vector>
#include <climits>
#include <unordered_map>
#include "Item.h"

class Branch {
public:
    ///
    /// \param _c Capacity
    /// \param _items
    Branch(int _c, std::vector<Item> &_items) : c(_c), items(_items), indexOfItem(0),
                                                reduced(0),
                                                distribution(std::vector<int>(_items.size(), 0)) {}

    Branch(int _c, int _reduced, int _indexOfItem,
           std::vector<Item> &_items, std::vector<int> &_distribution) :
            c(_c), reduced(_reduced), indexOfItem(_indexOfItem), items(std::move(_items)),
            distribution(std::move(_distribution)) {}


    ///Lower bound L2
    int lowerBound2() const;

    ///Lower bound L3
    int lowerBound3() const;

    ///Delete items that have been determined to have no better solution from list by algorithm MTRP
    void reduction();

    /// Calculate the upper bound by algorithm BFD(Best Fit Decreasing)
    /// \param currSolution For storing the solution
    /// \return Upper bound
    int upperBound(std::vector<int> &currSolution);

    void mergeTwoItems(int index1, int index2);

    std::vector<int> &getDistribution() { return distribution; }

    int getIndexOfItem() { return indexOfItem; }

    void incrementIndex() {
        ++indexOfItem;
        while (items[indexOfItem].weight + items[indexOfItem - 1].weight > c) {
//            addCurrentItem();
            ++indexOfItem;
        }
    }

    int getReduced() { return reduced; }

    std::vector<Item> &getItems() { return items; }

    std::vector<int> serialization();

//    void addCurrentItem();

private:
    ///capacity
    int c;
    ///number of bins reduced by MTRP
    int reduced;
    ///Is a pointer to the unallocated object with the heaviest weight
    int indexOfItem;

    std::vector<Item> items;
    std::vector<int> distribution;
    //if distribution==0 -> not distributed this item
    //If not equal to zero, this number is the index of bin

    int L2withA(int alpha) const;

    int findLargestK(int index) const;

    int findMinH(int index) const;

    bool findAB(int index, int &a, int &b) const;

    void printDistribution() const;
};

#endif //BINPACKING_BNB_BOUND_H
