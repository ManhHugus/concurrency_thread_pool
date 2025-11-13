#ifndef THREAD_POOLS_
#define THREAD_POOLS_

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <iostream>
#include "thread_safe_queue.hpp"

#define INITIAL_THREAD_VECTOR_SIZE 16

typedef enum {
    LOW_PRIORITY = 0,
    MEDIUM_PRIORITY,
    HIGH_PRIORITY
} priority_t;

typedef struct {
    priority_t task_priority;
    std::function<void()> task_function;
} task_attributes_t;

class thread_pool 
{
    private:
        thread_safe_queue<task_attributes_t> priority_work_queue;
        thread_safe_queue<std::function<void()>> work_queue;
        std::vector<std::thread> threads;
        std::atomic<bool> stop = false;

        void worker_task(void);
    public: 
        explicit thread_pool(size_t original_vector_size = 16);
        // void enqueue_task(std::function<void()> desired_task);

        template<typename FunctionType, typename... Args>
        auto submit_task(FunctionType&& f, Args&&... args)
        -> std::future<typename std::invoke_result<FunctionType, Args...>::type>
        {
            using return_type = typename std::invoke_result<FunctionType, Args...>::type;

            // Create a packaged_task with bound arguments
            auto task_ptr = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<FunctionType>(f), std::forward<Args>(args)...)
            );

            std::future<return_type> res = task_ptr->get_future();

            // Push a void() lambda that calls the packaged_task
            work_queue.push([task_ptr]() { (*task_ptr)(); });
            work_queue.notify_data_condition();

            return res;
        }

        thread_pool(const thread_pool&) = delete;
        thread_pool& operator=(const thread_pool&) = delete;
        ~thread_pool();
};

#endif