#include "thread_pools.hpp"

thread_pool::thread_pool(size_t original_vector_size) : stop(false)
{
    for (size_t index = 0; index < original_vector_size; index++) {
        worker_threads.emplace_back([this]{this->worker_task();});
    }
}

void thread_pool::worker_task(void)
{
    while (!stop)
    {
        task_attributes_t task_attr;
        priority_work_queue.wait_and_pop(task_attr,
             [this]{ return this->stop.load(std::memory_order_acquire);});
        try {
            task_attr.task_function();
        }
        catch(const std::exception& e) {
            std::cerr << e.what() << '\n';
        }
    }
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