#include "thread_pools.hpp"

thread_pool::thread_pool(size_t original_vector_size)
{
    this->thread_quantity = original_vector_size;

    for (size_t index = 0; index < original_vector_size; index++) {
        this->thread_vector.emplace_back([this]{ this->worker_task(); });
    }
}

void thread_pool::resize_thread_pool(uint32_t inputed_size)
{
    if (inputed_size == this->thread_vector.size()) {
        return;
    }

    if (inputed_size < this->thread_vector.size()) {

    } else {

    }
    this->thread_vector.resize(inputed_size);
}

void thread_pool::check_task_queue() const
{
}

void thread_pool::enqueue_task(std::function<void()> desired_task)
{
    std::lock_guard<std::mutex> mutex_lock(queue_mutex);
    this->task_queue.push(desired_task);
    this->task_condition.notify_one();
}

void thread_pool::dequeue_task()
{
    std::lock_guard<std::mutex> mutex_lock(queue_mutex);
    this->task_queue.pop();
}

void thread_pool::worker_task(void)
{
    std::function<void()> task; 

    while (true) 
    {
        {
            std::unique_lock<std::mutex> mutex_lock(queue_mutex);
            this->task_condition.wait(mutex_lock, [this]{return stop || !this->task_queue.empty();});
            if (stop == true && this->task_queue.empty()) 
                return; 
            task = this->task_queue.front();
            this->task_queue.pop();
            mutex_lock.unlock();
        }
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
    this->stop = true;
    task_condition.notify_all(); 
    for (size_t index = 0; index < this->thread_vector.size(); index++) {
        if (thread_vector[index].joinable()) {
            thread_vector[index].join();
        }
    }
}