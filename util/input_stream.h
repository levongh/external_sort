#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

namespace external_sort {

template <typename Block, typename Reader, typename MemoryPolicy>
class BlockInputStream : public Reader, public MemoryPolicy
{
public:
    using BlockType = Block;
    using BlockPtr  = Block*;
    using Iterator  = typename Block::iterator;
    using ValueType = typename Block::value_type;

    void Open();
    void Close();
    bool Empty();

    ValueType& Front();     // get a single value
    BlockPtr FrontBlock();  // get entire block
    BlockPtr ReadBlock();   // read a block right from the file

    void Pop();
    void PopBlock();

private:
    void InputLoop();
    void WaitForBlock();

private:
    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    std::queue<BlockPtr> blocks_queue_;

    BlockPtr block_ = {nullptr};
    Iterator block_iter_;

    std::thread tinput_;
    std::atomic<bool> empty_ = {false};
};

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::Open()
{
    Reader::Open();
    empty_ = false;
    tinput_ = std::thread(&BlockInputStream::InputLoop, this);
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::Close()
{
    Reader::Close();
    tinput_.join();
}

template <typename Block, typename Reader, typename MemoryPolicy>
bool BlockInputStream<Block, Reader, MemoryPolicy>::Empty()
{
    if (!block_) {
        WaitForBlock();
    }
    return empty_ && !block_;
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::Front()
    -> ValueType&
{
    return *block_iter_;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::Pop()
{
    ++block_iter_;
    if (block_iter_ == block_->end()) {
        auto tmp = block_;
        PopBlock();
        MemoryPolicy::Free(tmp);
    }
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::FrontBlock()
    -> BlockPtr
{
    return block_;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::PopBlock()
{
    block_ = nullptr;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::InputLoop()
{
    while (!Reader::Empty()) {
        BlockPtr block = ReadBlock();

        if (block) {
            std::unique_lock<std::mutex> lck(mtx_);
            blocks_queue_.push(block);
            cv_.notify_one();
        }
    }

    std::unique_lock<std::mutex> lck(mtx_);
    empty_ = true;
    cv_.notify_one();
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::ReadBlock()
    -> BlockPtr
{
    BlockPtr block = MemoryPolicy::Allocate();

    Reader::Read(block);
    if (block->empty()) {
        MemoryPolicy::Free(block);
        block = nullptr;
    }

    return block;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::WaitForBlock()
{
    std::unique_lock<std::mutex> lck(mtx_);
    while (blocks_queue_.empty() && !empty_) {
        cv_.wait(lck);
    }

    if (!blocks_queue_.empty()) {
        block_ = blocks_queue_.front();
        blocks_queue_.pop();
        block_iter_ = block_->begin();
    }
}

} // namespace external_sort
