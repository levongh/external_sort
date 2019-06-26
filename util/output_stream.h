#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

#include "block_types.h"

namespace external_sort {

template <typename Block, typename Writer, typename MemoryPolicy>
class BlockOutputStream : public Writer, public MemoryPolicy
{
  public:
    using BlockType = Block;
    using BlockPtr  = typename BlockTraits<Block>::BlockPtr;
    using Iterator  = typename BlockTraits<Block>::Iterator;
    using ValueType = typename BlockTraits<Block>::ValueType;

    void Open();
    void Close();

    void Push(const ValueType& value);  // push a single value
    void PushBlock(BlockPtr block);     // push entire block
    void WriteBlock(BlockPtr block);    // write a block directly into a file

  private:
    void OutputLoop();

  private:

    mutable std::condition_variable cv_;
    mutable std::mutex mtx_;
    std::queue<BlockPtr> blocks_queue_;

    BlockPtr block_ = {nullptr};

    std::thread toutput_;
    std::atomic<bool> stopped_ = {false};
};

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Open()
{
    Writer::Open();
    stopped_ = false;
    toutput_ = std::thread(&BlockOutputStream::OutputLoop, this);
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Close()
{
    PushBlock(block_);
    stopped_ = true;
    cv_.notify_one();
    toutput_.join();
    Writer::Close();
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Push(
    const ValueType& value)
{
    if (!block_) {
        block_ = MemoryPolicy::Allocate();
    }
    block_->push_back(value);

    if (block_->size() == block_->capacity()) {
        // block is full, push it to the output queue
        PushBlock(block_);
        block_ = nullptr;
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::PushBlock(
    BlockPtr block)
{
    if (block) {
        std::unique_lock<std::mutex> lck(mtx_);
        blocks_queue_.push(block);
        cv_.notify_one();
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::OutputLoop()
{
    for (;;) {
        std::unique_lock<std::mutex> lck(mtx_);
        while (blocks_queue_.empty() && !stopped_) {
            cv_.wait(lck);
        }

        if (!blocks_queue_.empty()) {
            BlockPtr block = blocks_queue_.front();
            blocks_queue_.pop();
            lck.unlock();

            WriteBlock(block);
        } else if (stopped_) {
            break;
        }
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::WriteBlock(
    BlockPtr block)
{
    Writer::Write(block);
    MemoryPolicy::Free(block);
}

} // namespace external_sort
