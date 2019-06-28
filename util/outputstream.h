#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>

namespace external_sort {

template <typename Block, typename Writer, typename Memory>
class OutputStream : public Writer, public Memory
{
public:
    void open();
    void close();

    void push(const typename Block::value_type& value);
    void writeBlock(Block* block);

private:
    void loop();
    void push(Block* block);

private:

    mutable std::condition_variable m_cv;
    mutable std::mutex m_mutex;
    std::queue<Block*> m_queue;

    Block* m_block = {nullptr};

    std::thread toutput_;
    std::atomic<bool> stopped_ = {false};
};

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::open()
{
    this->open();
    stopped_ = false;
    toutput_ = std::thread(&OutputStream::loop, this);
}

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::close()
{
    push(m_block);
    stopped_ = true;
    m_cv.notify_one();
    toutput_.join();
    this->close();
}

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::push(
    const typename Block::value_type& value)
{
    if (!m_block) {
        m_block = this->allocate();
    }
    m_block->push_back(value);

    if (m_block->size() == m_block->capacity()) {
        push(m_block);
        m_block = nullptr;
    }
}

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::push(
    Block* block)
{
    if (block) {
        std::unique_lock<std::mutex> lck(m_mutex);
        m_queue.push(block);
        m_cv.notify_one();
    }
}

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::loop()
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

            writeBlock(block);
        } else if (stopped_) {
            break;
        }
    }
}

template <typename Block, typename Writer, typename Memory>
void OutputStream<Block, Writer, Memory>::writeBlock(
    Block* block)
{
    this->write(block);
    this->free(block);
}

} // namespace external_sort
