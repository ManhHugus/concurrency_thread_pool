#ifndef THREAD_SAFE_QUEUE_
#define THREAD_SAFE_QUEUE_

#include <thread>
#include <queue> 
#include <mutex> 
#include <condition_variable>

template<typename T> 
class thread_safe_queue 
{
    private: 
        mutable std::mutex thread_safe_queue_mutex;
        std::queue<T> data_queue;
        std::condition_variable data_condition;

    public:
        thread_safe_queue() {}
        void push(T new_value); 
        void wait_and_pop(T& value); 
        std::shared_ptr<T> wait_and_pop(); 
        bool try_pop(T& value); 
        std::shared_ptr<T> try_pop();
        bool empty() const;
};

#endif