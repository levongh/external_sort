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
    using BlockType  = Block;
    using Iterator  = typename Block::iterator;

    void open();
    void close();
    bool empty();

    typename Block::value_type& Front();
    Block* FrontBlock();
    Block* ReadBlock();

    void Pop();
    void PopBlock();

private:
    void InputLoop();
    void WaitForBlock();

private:
    mutable std::condition_variable m_cv;
    mutable std::mutex m_mtx;
    std::queue<Block*> m_queue;

    Block* m_block = {nullptr};
    Iterator m_blockIter;

    std::thread tinput_;
    std::atomic<bool> empty_ = {false};
};

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::open()
{
    Reader::open();
    empty_ = false;
    tinput_ = std::thread(&BlockInputStream::InputLoop, this);
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::close()
{
    Reader::close();
    tinput_.join();
}

template <typename Block, typename Reader, typename MemoryPolicy>
bool BlockInputStream<Block, Reader, MemoryPolicy>::empty()
{
    if (!m_block) {
        WaitForBlock();
    }
    return empty_ && !m_block;
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::Front()
    -> typename Block::value_type&
{
    return *m_blockIter;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::Pop()
{
    ++m_blockIter;
    if (m_blockIter == m_block->end()) {
        auto tmp = m_block;
        PopBlock();
        MemoryPolicy::free(tmp);
    }
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::FrontBlock()
    -> Block*
{
    return m_block;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::PopBlock()
{
    m_block = nullptr;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::InputLoop()
{
    while (!Reader::empty()) {
        Block* block = ReadBlock();

        if (block) {
            std::unique_lock<std::mutex> lck(m_mtx);
            m_queue.push(block);
            m_cv.notify_one();
        }
    }

    std::unique_lock<std::mutex> lck(m_mtx);
    empty_ = true;
    m_cv.notify_one();
}

template <typename Block, typename Reader, typename MemoryPolicy>
auto BlockInputStream<Block, Reader, MemoryPolicy>::ReadBlock()
    -> Block*
{
    Block* block = MemoryPolicy::allocate();

    Reader::read(block);
    if (block->empty()) {
        MemoryPolicy::free(block);
        block = nullptr;
    }

    return block;
}

template <typename Block, typename Reader, typename MemoryPolicy>
void BlockInputStream<Block, Reader, MemoryPolicy>::WaitForBlock()
{
    std::unique_lock<std::mutex> lck(m_mtx);
    while (m_queue.empty() && !empty_) {
        m_cv.wait(lck);
    }

    if (!m_queue.empty()) {
        m_block = m_queue.front();
        m_queue.pop();
        m_blockIter = m_block->begin();
    }
}

} // namespace external_sort
