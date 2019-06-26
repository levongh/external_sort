#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <list>

namespace external_sort {

template <typename ResultType>
class AsyncFuncs
{
public:
    template <class Fn, class... Args>
    void Async(Fn&& fn, Args&&... args)
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        ++m_funcsRunning;
        std::thread task(&AsyncFuncs::RunFunc<Fn, Args...>, this,
                std::forward<Fn>(fn), std::forward<Args>(args)...);
        task.detach();
    }

    ResultType get()
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        while (m_funcsReady.empty()) {
            m_cv.wait(lck);
        }

        ResultType result = m_funcsReady.front();
        m_funcsReady.pop_front();
        return result;
    }

    bool Empty() const
    {
        return All() == 0;
    }

    size_t All() const
    {
        return Ready() + Running();
    }

    size_t Ready() const
    {
        return m_funcsRunning;
    }

    size_t Running() const
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        return m_funcsReady.size();
    }

private:
    template <class Fn, class... Args>
    void RunFunc(Fn&& fn, Args&&... args)
    {
        ResultType result = fn(std::forward<Args>(args)...);

        std::unique_lock<std::mutex> lck(m_mutex);
        m_funcsReady.push_back(result);
        --m_funcsRunning;
        m_cv.notify_one();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;

    std::atomic<size_t> m_funcsRunning = {0};
    std::list<ResultType> m_funcsReady;
};

} // namespace external_sort

