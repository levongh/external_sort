#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

namespace external_sort {

template <typename Block>
class InputStream : public FileReader<Block>, public Allocator<Block>
{
public:
    using BlockType  = Block;
    using Iterator  = typename Block::iterator;

    void open();
    void close();
    bool empty();

    typename Block::value_type& front();
    Block* block();

    void pop();
    void null();

private:
    Block* read();
    void loop();
    void waitBlock();

private:
    mutable std::condition_variable m_cv;
    mutable std::mutex m_mtx;
    std::queue<Block*> m_queue;

    Block* m_block = {nullptr};
    Iterator m_blockIter;

    std::thread tinput_;
    std::atomic<bool> empty_ = {false};
};

template <typename Block>
void InputStream<Block>::open()
{
    FileReader<Block>::open();
    empty_ = false;
    tinput_ = std::thread(&InputStream::loop, this);
}

template <typename Block>
void InputStream<Block>::close()
{
    this->close();
    tinput_.join();
}

template <typename Block>
bool InputStream<Block>::empty()
{
    if (!m_block) {
        waitBlock();
    }
    return empty_ && !m_block;
}

template <typename Block>
auto InputStream<Block>::front()
    -> typename Block::value_type&
{
    return *m_blockIter;
}

template <typename Block>
void InputStream<Block>::pop()
{
    ++m_blockIter;
    if (m_blockIter == m_block->end()) {
        auto tmp = m_block;
        null();
        this->free(tmp);
    }
}

template <typename Block>
auto InputStream<Block>::block()
    -> Block*
{
    return m_block;
}

template <typename Block>
void InputStream<Block>::null()
{
    m_block = nullptr;
}

template <typename Block>
void InputStream<Block>::loop()
{
    while (!FileReader<Block>::empty()) {
        Block* block = read();

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

template <typename Block>
auto InputStream<Block>::read() -> Block*
{
    Block* block = this->allocate();

    FileReader<Block>::read(block);
    if (block->empty()) {
        this->free(block);
        block = nullptr;
    }

    return block;
}

template <typename Block>
void InputStream<Block>::waitBlock()
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
