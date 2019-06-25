#pragma once

#include <vector>
#include <memory>

namespace external_sort
{

template <typename T>
using VectorBlock = std::vector<T>;

template <typename BlockType>
struct BlockTraits
{
    using Block = BlockType;
    using BlockPtr = Block*;

    inline static void* RawPtr(BlockPtr block)
    {
        return block;
    }

    inline static void DeletePtr(BlockPtr block)
    {
        delete block;
    };

    using Container = Block;
    using Iterator  = typename Container::iterator;
    using ValueType = typename Container::value_type;
};

} // namespace external_sort

