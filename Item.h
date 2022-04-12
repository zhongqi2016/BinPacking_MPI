//
// Created by 吴中奇 on 2021/10/31.
//

#ifndef BINPACKING_BNB_ITEM_H
#define BINPACKING_BNB_ITEM_H

#include <vector>

struct Item {
    int index;
    int weight;
//    bool combination;

    Item(int _index, int _weight) : index(_index), weight(_weight) {}

    bool operator<(const Item &item) const {
        return weight < item.weight;
    }
};

std::vector<Item> refactor(std::vector<int> &inputItems);

#endif //BINPACKING_BNB_ITEM_H
