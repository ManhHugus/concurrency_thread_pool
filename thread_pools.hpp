#ifndef THREAD_POOLS_
#define THREAD_POOLS_

#include <vector>
#include <future>
#include <atomic>
#include <iostream>
#include <functional>
#include "work_stealing_queue.hpp"
#include "thread_safe_queue.hpp"

#define INITIAL_THREAD_VECTOR_SIZE 16

class thread_pool 
{
    private:
        std::atomic<uint32_t> next_task_sequence_number{0};
        thread_safe_queue<task_attributes_t> priority_work_queue;
        std::vector<std::thread> worker_threads;
        std::atomic<bool> stop = false;

        std::vector<std::unique_ptr<work_stealing_queue<task_attributes_t>>> worker_task_queues;

        static thread_local work_stealing_queue<task_attributes_t>* local_work_stealing_queue;
        static thread_local unsigned local_thread_index;

        void worker_task(unsigned thread_index);
        bool pop_task_from_local_queue(task_attributes_t& task_attr);
        bool pop_task_from_pool_queue(task_attributes_t& task_attr);
        bool pop_task_from_other_queues(task_attributes_t& task_attr);
        
    public: 
        explicit thread_pool(size_t original_vector_size = 16);

        template<typename FunctionType, typename... Args>
        auto submit_task(priority_t task_priority, FunctionType&& f, Args&&... args)
        -> std::future<typename std::invoke_result<FunctionType, Args...>::type>
        {
            using return_type = typename std::invoke_result<FunctionType, Args...>::type;

            // Create a packaged_task with bound arguments
            auto task_ptr = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<FunctionType>(f), std::forward<Args>(args)...));

            std::future<return_type> res = task_ptr->get_future();

            task_attributes_t task_attributes;
            task_attributes.task_sequence_number = next_task_sequence_number.fetch_add(1);
            task_attributes.task_priority = task_priority;
            task_attributes.task_function = [task_ptr]() { (*task_ptr)(); };

            // Push a void() lambda that calls the packaged_task
            priority_work_queue.push(std::move(task_attributes));

            return res;
        }

        thread_pool(const thread_pool&) = delete;
        thread_pool& operator=(const thread_pool&) = delete;
        ~thread_pool();
};

#endif