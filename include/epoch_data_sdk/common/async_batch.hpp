#pragma once

#include <vector>
#include <expected>
#include <drogon/drogon.h>
#include <atomic>
#include <coroutine>

namespace data_sdk::common {

// Forward declarations
template <typename T>
struct WhenAllAwaiter;

// Base template for WhenAllAwaiter - handles tasks that return values
template <typename T>
struct WhenAllAwaiter : public drogon::CallbackAwaiter<std::vector<T>>
{
  WhenAllAwaiter(std::vector<drogon::Task<T>> &&t)
      : tasks_(std::move(t)),
        results_(tasks_.size()),
        counter_(tasks_.size())
  {
  }

  void await_suspend(std::coroutine_handle<> handle)
  {
    if (tasks_.empty())
    {
      handle.resume();
      return;
    }

    const size_t count = tasks_.size();
    for (size_t i = 0; i < count; ++i)
    {
      // Launch each task concurrently
      [](WhenAllAwaiter *self,
         std::coroutine_handle<> handle,
         drogon::Task<T> task,
         size_t index) -> drogon::AsyncTask {
        try
        {
          // Execute task and store result at correct index
          self->results_[index] = co_await task;
        }
        catch (...)
        {
          if (self->exceptionFlag_.test_and_set() == false)
            self->exception_ = std::current_exception();
        }
        // Decrement counter; if we're the last one, resume caller
        if (self->counter_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
          // This line CAN delete `this` at last iteration. We MUST
          // NOT depend on `this` after this point
          handle.resume();
        }
      }(this, handle, std::move(tasks_[i]), i);
    }
  }

  std::vector<T> await_resume()
  {
    if (exception_)
      std::rethrow_exception(exception_);
    return std::move(results_);
  }

  std::vector<drogon::Task<T>> tasks_;
  std::vector<T> results_;
  std::atomic<size_t> counter_;
  std::atomic_flag exceptionFlag_ = ATOMIC_FLAG_INIT;
  std::exception_ptr exception_{nullptr};
};

// Specialization for void tasks
template <>
struct WhenAllAwaiter<void> : public drogon::CallbackAwaiter<void>
{
  WhenAllAwaiter(std::vector<drogon::Task<void>> &&t)
      : tasks_(std::move(t)), counter_(tasks_.size())
  {
  }

  void await_suspend(std::coroutine_handle<> handle)
  {
    if (tasks_.empty())
    {
      handle.resume();
      return;
    }

    const size_t count = tasks_.size();
    for (size_t i = 0; i < count; ++i)
    {
      [](WhenAllAwaiter *self,
         std::coroutine_handle<> handle,
         drogon::Task<void> task) -> drogon::AsyncTask {
        try
        {
          co_await task;
        }
        catch (...)
        {
          if (self->exceptionFlag_.test_and_set() == false)
            self->exception_ = std::current_exception();
        }
        if (self->counter_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
          // This line CAN delete `this` at last iteration. We MUST
          // NOT depend on `this` after this point
          handle.resume();
        }
      }(this, handle, std::move(tasks_[i]));
    }
  }

  void await_resume()
  {
    if (exception_)
      std::rethrow_exception(exception_);
  }

  std::vector<drogon::Task<void>> tasks_;
  std::atomic<size_t> counter_;
  std::atomic_flag exceptionFlag_ = ATOMIC_FLAG_INIT;
  std::exception_ptr exception_{nullptr};
};

// Generic async batch utilities for joining multiple coroutine operations
// These utilities allow executing multiple async operations CONCURRENTLY
// and collecting their results in order.

// Execute multiple tasks concurrently and return results in original order
// All tasks run in parallel, and results are collected in the same order as input tasks
//
// Example:
//   std::vector<drogon::Task<Expected<DataFrame>>> tasks;
//   tasks.push_back(client.getDataAsync("A"));
//   tasks.push_back(client.getDataAsync("B"));
//   auto results = co_await when_all(std::move(tasks));
//   // results[0] corresponds to task[0], results[1] to task[1], etc.
template<typename T>
inline drogon::Task<std::vector<T>> when_all(std::vector<drogon::Task<T>> tasks) {
  co_return co_await WhenAllAwaiter<T>(std::move(tasks));
}

// Synchronous wrapper for when_all - blocks until all tasks complete
// Useful when you want concurrent execution but need to call from synchronous code
//
// Example:
//   std::vector<drogon::Task<Expected<DataFrame>>> tasks;
//   tasks.push_back(client.getAggregatesAsync("AAPL", from, to, true));
//   tasks.push_back(client.getAggregatesAsync("MSFT", from, to, true));
//   auto results = syncWhenAll(std::move(tasks));
//   // Blocks until both complete, returns vector in order
template<typename T>
inline std::vector<T> syncWhenAll(std::vector<drogon::Task<T>> tasks) {
  return drogon::sync_wait(when_all(std::move(tasks)));
}

// Legacy aliases for backward compatibility
template<typename T>
inline drogon::Task<std::vector<T>> joinAll(std::vector<drogon::Task<T>> tasks) {
  return when_all(std::move(tasks));
}

template<typename T>
inline std::vector<T> syncJoinAll(std::vector<drogon::Task<T>> tasks) {
  return syncWhenAll(std::move(tasks));
}

} // namespace data_sdk::common
