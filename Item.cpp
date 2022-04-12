//
// Created by 吴中奇 on 2021/10/31.
//
#include "Item.h"

std::vector<Item> refactor(std::vector<int> &inputItems) {
    std::vector<Item> output;
    output.reserve(inputItems.size());
    for (int i = 0; i < inputItems.size(); ++i) {
        Item item(i, inputItems[i]);
        output.emplace_back(item);
    }
    return output;
}