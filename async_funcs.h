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
    void Async(Fn&& fn, Args&&... args);

    ResultType GetAny();

    bool Empty() const
    {
        return All() == 0;
    }

    size_t All() const
    {
        return Ready() + Running();
    }

    size_t Ready() const;
    size_t Running() const;

private:
    template <class Fn, class... Args>
    void RunFunc(Fn&& fn, Args&&... args);

private:
    mutable std::mutex mtx_;
    std::condition_variable cv_;

    std::atomic<size_t> funcs_running_ = {0};
    std::list<ResultType> funcs_ready_;
};

template <typename ResultType>
size_t AsyncFuncs<ResultType>::Running() const
{
    return funcs_running_;
}

template <typename ResultType>
size_t AsyncFuncs<ResultType>::Ready() const
{
    std::unique_lock<std::mutex> lck(mtx_);
    return funcs_ready_.size();
}

template <typename ResultType>
ResultType AsyncFuncs<ResultType>::GetAny()
{
    std::unique_lock<std::mutex> lck(mtx_);
    while (funcs_ready_.empty()) {
        cv_.wait(lck);
    }

    ResultType result = funcs_ready_.front();
    funcs_ready_.pop_front();
    return result;
}

template <typename ResultType>
template <class Fn, class... Args>
void AsyncFuncs<ResultType>::Async(Fn&& fn, Args&&... args)
{
    std::unique_lock<std::mutex> lck(mtx_);
    funcs_running_++;
    std::thread task(&AsyncFuncs::RunFunc<Fn, Args...>, this,
                     std::forward<Fn>(fn), std::forward<Args>(args)...);
    task.detach();
}

template <typename ResultType>
template <class Fn, class... Args>
void AsyncFuncs<ResultType>::RunFunc(Fn&& fn, Args&&... args)
{
    ResultType result = fn(std::forward<Args>(args)...);

    std::unique_lock<std::mutex> lck(mtx_);
    funcs_ready_.push_back(result);
    funcs_running_--;
    cv_.notify_one();
}

} // namespace external_sort

