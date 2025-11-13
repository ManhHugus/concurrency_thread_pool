#include "thread_pools.hpp"

thread_pool::thread_pool(size_t original_vector_size) : stop(false)
{
    for (size_t index = 0; index < original_vector_size; index++) {
        threads.emplace_back([this]{this->worker_task();});
    }
}

void thread_pool::worker_task(void)
{
    while (!stop)
    {
        std::function<void()> task; 
        work_queue.wait_and_pop(task, [this]{ return this->stop.load(std::memory_order_acquire);});
        try {
            task();
        }
        catch(const std::exception& e) {
            std::cerr << e.what() << '\n';
        } 
    }   
}
    
thread_pool::~thread_pool() 
{
    stop.store(true, std::memory_order_release);
    work_queue.notify_all_data_condition();
    for (size_t index = 0; index < threads.size(); index++) {
        if (threads[index].joinable()) {
            threads[index].join();
        }
    }
}