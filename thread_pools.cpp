#include "thread_pools.hpp"

explicit thread_pool::thread_pool(size_t original_vector_size = INITIAL_THREAD_VECTOR_SIZE)
{
    this->thread_quantity = original_vector_size;

    for (size_t index = 0; index < original_vector_size; index++) {
        this->thread_vector.emplace_back([this]{ this->worker_task(); });
    }

    while (!this->task_queue.empty()) {
        this->task_queue.pop();
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

void thread_pool::check_task_queue()
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
    bool running = true;
    std::function<void()> task; 

    while (running) 
    {
        {
            std::unique_lock<std::mutex> mutex_lock(queue_mutex);
            this->task_condition.wait(mutex_lock, [this]{return stop || !this->task_queue.empty();});
            if (stop == true && this->task_queue.empty()) 
                return; 
            task = this->task_queue.front();
            mutex_lock.unlock();
            this->dequeue_task();
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
    for (size_t index; index < this->thread_vector.size(); index++) {
        if (thread_vector[index].joinable()) {
            thread_vector[index].join();
        }
    }
}