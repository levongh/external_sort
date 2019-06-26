#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <stack>
#include <cassert>

namespace external_sort {

template <typename Block>
class BlockMemoryPolicy
{
public:
    using BlockPtr = Block*;
    class BlockPool;
    using BlockPoolPtr = std::shared_ptr<BlockPool>;

    class BlockPool
    {
    public:
        BlockPool(size_t memsize, size_t memblocks)
            : m_blocks(memblocks)
            , m_blocksAllocated(0)
        {
            size_t block_size = memsize / memblocks /
                (sizeof(typename Block::value_type));

            while (m_pool.size() < m_blocks) {
                BlockPtr block(new Block);
                block->reserve(block_size);
                m_pool.push(block);
            }
        }

        ~BlockPool()
        {
            while (!m_pool.empty()) {
                BlockPtr block = m_pool.top();
                delete block;
                m_pool.pop();
            }
        }

    public:
        size_t allocated() const
        {
            std::unique_lock<std::mutex> lck(m_mutex);
            return m_blocksAllocated;
        }

        BlockPtr allocate()
        {
            std::unique_lock<std::mutex> lck(m_mutex);
            while (m_pool.empty()) {
                m_cv.wait(lck);
            }
            BlockPtr block = m_pool.top();
            m_pool.pop();
            ++m_blocksAllocated;
            return block;
        }

        void free(BlockPtr block)
        {
            std::unique_lock<std::mutex> lck(m_mutex);
            --m_blocksAllocated;
            block->resize(0);
            m_pool.push(block);
            m_cv.notify_one();
        }

    private:
        mutable std::mutex m_mutex;
        std::condition_variable m_cv;
        std::stack<BlockPtr> m_pool;
        size_t m_blocks;
        size_t m_blocksAllocated;
    };

public:
    inline size_t allocated() const
    {
        return m_memPool->allocated();
    }

    inline BlockPtr allocate()
    {
        return m_memPool->allocate();
    }

    inline void free(BlockPtr block)
    {
        m_memPool->free(block);
    }

    void set_mem_pool(size_t memsize, size_t memblocks)
    {
        m_memPool = std::make_shared<BlockPool>(memsize, memblocks);
    }

    void set_mem_pool(BlockPoolPtr pool)
    {
        m_memPool = pool;
    }

private:
    BlockPoolPtr m_memPool = {nullptr};
};

} // namespace external_sort
