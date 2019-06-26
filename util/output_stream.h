#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

namespace external_sort {

template <typename Block, typename Writer, typename MemoryPolicy>
class BlockOutputStream : public Writer, public MemoryPolicy
{
public:
    void Open();
    void Close();

    void Push(const typename Block::value_type& value);
    void PushBlock(Block* block);
    void WriteBlock(Block* block);

private:
    void OutputLoop();

private:

    mutable std::condition_variable m_cv;
    mutable std::mutex m_mutex;
    std::queue<Block*> m_queue;

    Block* m_block = {nullptr};

    std::thread toutput_;
    std::atomic<bool> stopped_ = {false};
};

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Open()
{
    Writer::open();
    stopped_ = false;
    toutput_ = std::thread(&BlockOutputStream::OutputLoop, this);
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Close()
{
    PushBlock(m_block);
    stopped_ = true;
    m_cv.notify_one();
    toutput_.join();
    Writer::close();
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::Push(
    const typename Block::value_type& value)
{
    if (!m_block) {
        m_block = MemoryPolicy::allocate();
    }
    m_block->push_back(value);

    if (m_block->size() == m_block->capacity()) {
        PushBlock(m_block);
        m_block = nullptr;
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::PushBlock(
    Block* block)
{
    if (block) {
        std::unique_lock<std::mutex> lck(m_mutex);
        m_queue.push(block);
        m_cv.notify_one();
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::OutputLoop()
{
    for (;;) {
        std::unique_lock<std::mutex> lck(m_mutex);
        while (m_queue.empty() && !stopped_) {
            m_cv.wait(lck);
        }

        if (!m_queue.empty()) {
            Block* block = m_queue.front();
            m_queue.pop();
            lck.unlock();

            WriteBlock(block);
        } else if (stopped_) {
            break;
        }
    }
}

template <typename Block, typename Writer, typename MemoryPolicy>
void BlockOutputStream<Block, Writer, MemoryPolicy>::WriteBlock(
    Block* block)
{
    Writer::write(block);
    MemoryPolicy::free(block);
}

} // namespace external_sort
