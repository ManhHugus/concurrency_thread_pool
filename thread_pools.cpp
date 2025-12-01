#include "thread_pools.hpp"

thread_pool::thread_pool(size_t original_vector_size) : stop(false)
{
    try
    {
        for (unsigned index = 0; index < original_vector_size; index++) {
            worker_task_queue.push_back(std::make_unique<work_stealing_queue<task_attributes_t>>());
            worker_threads.push_back(std::thread(&thread_pool::worker_task, this, index));
        }
    }
    catch(...)
    {
        stop.store(true, std::memory_order_relaxed);
        throw;
    }
}

void thread_pool::worker_task(unsigned thread_index)
{
    local_thread_index = thread_index;
    local_work_stealing_queue = worker_task_queue[thread_index].get();
    task_attributes_t task_attr;
    while (!stop) {
        if (pop_task_from_local_queue(task_attr) ||
            pop_task_from_pool_queue(task_attr) ||
            pop_task_from_other_queues(task_attr)) {
            try {
                task_attr.task_function();
            }
            catch(const std::exception& e) {
                std::cerr << e.what() << '\n';
            }
        } else {
            std::unique_lock<std::mutex> lock(pool_mutex);
            pool_condition_variable.wait(lock, [this]{ return stop.load() || 
                !priority_work_queue.empty() || 
                !local_work_stealing_queue->empty(); });
        }
    }
}

bool thread_pool::pop_task_from_local_queue(task_attributes_t& task_attr)
{
    return (local_work_stealing_queue && !local_work_stealing_queue->empty()) ? 
        local_work_stealing_queue->pop_bottom(task_attr), true : false;
}

bool thread_pool::pop_task_from_pool_queue(task_attributes_t& task_attr)
{
    return priority_work_queue.try_pop(task_attr);
}

bool thread_pool::pop_task_from_other_queues(task_attributes_t& task_attr)
{
    for (size_t i = 0; i < worker_task_queue.size(); i++) {
        unsigned const index = (local_thread_index + i + 1) % worker_task_queue.size(); // Avoid stealing from the first thread

        if (index == local_thread_index) {
            continue;
        }

        if (!worker_task_queue[index]->empty()) {
            worker_task_queue[index]->steal_top(task_attr);
            return true;
        }
    }
    return false;
}

void thread_pool::add_workers(size_t num_new_workers)
{
    // Implementation for adding workers
    for (int i = 0; i < num_new_workers; i++) {
        size_t new_thread_index = worker_threads.size();
        worker_task_queue.push_back(std::make_unique<work_stealing_queue<task_attributes_t>>());
        worker_threads.push_back(std::thread(&thread_pool::worker_task, this, new_thread_index));
    }
}

void thread_pool::remove_workers(size_t num_workers_to_remove)
{
    // Implementation for removing workers
    
    
}

void thread_pool::resize_pool(size_t new_size)
{
    // Implementation for resizing the pool
}

thread_pool::~thread_pool()
{
    stop.store(true, std::memory_order_release);
    priority_work_queue.notify_all_data_condition();
    for (size_t index = 0; index < worker_threads.size(); index++) {
        if (worker_threads[index].joinable()) {
            worker_threads[index].join();
        }
    }
}