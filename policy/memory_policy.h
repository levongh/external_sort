#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <stack>
#include <cassert>

#include "block_types.h"

namespace external_sort {

template <typename Block>
class BlockMemoryPolicy
{
public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    class BlockPool;
    using BlockPoolPtr = std::shared_ptr<BlockPool>;

    class BlockPool
    {
    public:
        BlockPool(size_t memsize, size_t memblocks);
        ~BlockPool();

    public:
        size_t Allocated() const;
        BlockPtr Allocate();
        void Free(BlockPtr block);

    private:
        mutable std::mutex mtx_;
        std::condition_variable cv_;
        std::stack<BlockPtr> pool_;
        size_t blocks_;
        size_t blocks_cnt_;
        size_t blocks_allocated_;
    };

    inline size_t Allocated() const { return mem_pool_->Allocated(); }
    inline BlockPtr Allocate() { return mem_pool_->Allocate(); }
    inline void Free(BlockPtr block) { mem_pool_->Free(block); }

    BlockPoolPtr mem_pool() { return mem_pool_; }
    void set_mem_pool(size_t memsize, size_t memblocks) {
        mem_pool_ = std::make_shared<BlockPool>(memsize, memblocks);
    };
    void set_mem_pool(BlockPoolPtr pool) { mem_pool_ = pool; };

private:
    BlockPoolPtr mem_pool_ = {nullptr};
};

template <typename Block>
BlockMemoryPolicy<Block>::BlockPool::BlockPool(size_t memsize,
                                               size_t memblocks)
    : blocks_(memblocks),
      blocks_cnt_(0),
      blocks_allocated_(0)
{
    size_t block_size = memsize / memblocks /
                        (sizeof(typename BlockTraits<Block>::ValueType));

    while (pool_.size() < blocks_) {
        BlockPtr block(new Block);
        block->reserve(block_size);
        pool_.push(block);
    }
}

template <typename Block>
BlockMemoryPolicy<Block>::BlockPool::~BlockPool()
{
    while (!pool_.empty()) {
        BlockPtr block = pool_.top();
        BlockTraits<Block>::DeletePtr(block);
        pool_.pop();
    }
    assert(blocks_allocated_ == 0);
}

template <typename Block>
size_t BlockMemoryPolicy<Block>::BlockPool::Allocated() const
{
    std::unique_lock<std::mutex> lck(mtx_);
    return blocks_allocated_;
}

template <typename Block>
auto BlockMemoryPolicy<Block>::BlockPool::Allocate()
    -> BlockPtr
{
    std::unique_lock<std::mutex> lck(mtx_);
    blocks_cnt_++;

    while (pool_.empty()) {
        cv_.wait(lck);
    }
    BlockPtr block = pool_.top();
    pool_.pop();

    blocks_allocated_++;
    return block;
}

template <typename Block>
void BlockMemoryPolicy<Block>::BlockPool::Free(BlockPtr block)
{
    std::unique_lock<std::mutex> lck(mtx_);
    blocks_allocated_--;

    block->resize(0);
    pool_.push(block);

    cv_.notify_one();
}

} // namespace external_sort
