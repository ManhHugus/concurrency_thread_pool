#include "thread_safe_queue.hpp"

template<typename T>
void thread_safe_queue<T>::push(T new_value) 
{
    std::lock_guard<std::mutex> lock(this->thread_safe_queue_mutex);
    this->data_queue.push(std::move(new_value));
    this->data_condition.notify_one();
}

template<typename T> 
std::shared_ptr<T> thread_safe_queue<T>::try_pop() 
{
    std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
    if (this->data_queue.empty()) {
        return std::shared_ptr<T>(); 
    }
    std::shared_ptr<T> return_shared_ptr_value(std::make_shared(std::move(data_queue.front())));
    data_queue.pop(); 
    return return_shared_ptr_value;
}

template <typename T>
bool thread_safe_queue<T>::try_pop(T& value) 
{
    std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
    if (this->data_queue.empty()) {
        return false; 
    }
    value = std::move(this->data_queue.front());
    this->data_queue.pop();
    return true;
}

template <typename T> 
std::shared_ptr<T> thread_safe_queue<T>::wait_and_pop() 
{
    std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
    this->data_condition.wait(this->thread_safe_queue_mutex, [this]{ return !this->data_queue.empty();});
    std::shared_ptr<T> return_shared_ptr_value(std::make_shared<T>(std::move(data_queue.front())));
    this->data_queue.pop();
    return return_shared_ptr_value;
}

template <typename T>
void thread_safe_queue<T>::wait_and_pop(T& value)
{
    std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
    this->data_condition.wait(this->thread_safe_queue_mutex, [this]{ return !this->data_queue.empty();});
    value = std::move(data_queue.front());
    this->data_queue.pop();
}

template<typename T>
bool thread_safe_queue<T>::empty() const 
{
    std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
    return this->data_queue.empty();
}