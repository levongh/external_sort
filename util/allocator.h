#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <stack>
#include <cassert>

namespace external_sort {

template <typename Block>
class Allocator
{
public:
    class BlockPool;

public:
    inline size_t allocated() const
    {
        return m_memPool->allocated();
    }

    inline Block* allocate()
    {
        return m_memPool->allocate();
    }

    inline void free(Block* block)
    {
        m_memPool->free(block);
    }

    void setPool(size_t memsize, size_t memblocks)
    {
        m_memPool = std::make_shared<BlockPool>(memsize, memblocks);
    }

    void setPool(std::shared_ptr<BlockPool> pool)
    {
        m_memPool = pool;
    }

private:
    std::shared_ptr<BlockPool> m_memPool = {nullptr};
};

#include "blockpool.h"

} // namespace external_sort
