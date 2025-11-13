#ifndef THREAD_SAFE_QUEUE_
#define THREAD_SAFE_QUEUE_

#include <thread>
#include <queue> 
#include <mutex> 
#include <condition_variable>

struct custom_comparator {
    bool operator()(const task_attributes_t& lhs, const task_attributes_t& rhs) const {
        return lhs.task_priority > rhs.task_priority; // Lower enum value has higher priority
    }
};

typedef enum {
    HIGH_PRIORITY = 0,
    MEDIUM_PRIORITY,
    LOW_PRIORITY
} priority_t;

typedef struct {
    priority_t task_priority;
    std::function<void()> task_function;
} task_attributes_t;

template<typename T> 
class thread_safe_queue 
{
    private: 
        mutable std::mutex thread_safe_queue_mutex;
        std::priority_queue<T, std::vector<T>, custom_comparator> priority_data_queue;
        std::queue<T> data_queue;
        std::condition_variable data_condition;

    public:
        thread_safe_queue() {}

        void notify_data_condition() {
            this->data_condition.notify_one();
        }

        void notify_all_data_condition() {
            this->data_condition.notify_all();
        }

        void push(T new_value)
        {
            std::lock_guard<std::mutex> lock(this->thread_safe_queue_mutex);
            this->data_queue.push(std::move(new_value));
            this->data_condition.notify_one();
        }

        void wait_and_pop(T& value, std::function<bool()> stop_condition)
        {
            std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
            this->data_condition.wait(lk, [this, &stop_condition](){ return stop_condition() || !this->data_queue.empty();});
            value = std::move(data_queue.front());
            this->data_queue.pop();
        } 

        std::shared_ptr<T> wait_and_pop()
        {
            std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
            this->data_condition.wait(lk, [this]{ return !this->data_queue.empty();});
            std::shared_ptr<T> return_shared_ptr_value(std::make_shared<T>(std::move(data_queue.front())));
            this->data_queue.pop();
            return return_shared_ptr_value;
        } 

        bool try_pop(T& value)
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            if (this->data_queue.empty()) {
                return false; 
            }
            value = std::move(this->data_queue.front());
            this->data_queue.pop();
            return true;
        }

        std::shared_ptr<T> try_pop()
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            if (this->data_queue.empty()) {
                return std::shared_ptr<T>();
            }
            std::shared_ptr<T> return_shared_ptr_value(std::make_shared(std::move(data_queue.front())));
            data_queue.pop();
            return return_shared_ptr_value;
        }

        bool empty() const 
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            return this->data_queue.empty();
        }
};

#endif