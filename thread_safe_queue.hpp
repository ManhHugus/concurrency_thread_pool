#ifndef THREAD_SAFE_QUEUE_
#define THREAD_SAFE_QUEUE_

#include <thread>
#include <queue> 
#include <mutex> 
#include <condition_variable>

typedef enum {
    HIGH_PRIORITY = 0,
    MEDIUM_PRIORITY,
    LOW_PRIORITY
} priority_t;

typedef struct {
    uint32_t task_sequence_number;
    priority_t task_priority;
    std::function<void()> task_function;
} task_attributes_t;

struct custom_comparator {
    bool operator()(const task_attributes_t& lhs, const task_attributes_t& rhs) const {
        if (lhs.task_priority == rhs.task_priority) {
            return lhs.task_sequence_number > rhs.task_sequence_number; // Earlier sequence number has higher priority
        }
        return lhs.task_priority > rhs.task_priority; // Lower enum value has higher priority
    }
};

template<typename T> 
class thread_safe_queue 
{
    private: 
        mutable std::mutex thread_safe_queue_mutex;
        std::priority_queue<T, std::vector<T>, custom_comparator> priority_data_queue;
        std::condition_variable data_condition;

    public:
        thread_safe_queue() {}

        void notify_data_condition() 
        {
            data_condition.notify_one();
        }

        void notify_all_data_condition() 
        {
            data_condition.notify_all();
        }

        void push(T new_value)
        {
            std::lock_guard<std::mutex> lock(this->thread_safe_queue_mutex);
            priority_data_queue.push(std::move(new_value));
            data_condition.notify_one();
        }

        void wait_and_pop(T& value, std::function<bool()> stop_condition)
        {
            std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
            data_condition.wait(lk, [this, &stop_condition](){ return stop_condition() || !this->priority_data_queue.empty();});
            value = std::move(priority_data_queue.top());
            priority_data_queue.pop();
        }

        std::shared_ptr<T> wait_and_pop(std::function<bool()> stop_condition)
        {
            std::unique_lock<std::mutex> lk(this->thread_safe_queue_mutex);
            data_condition.wait(lk, [this, &stop_condition]{ return stop_condition() || !this->priority_data_queue.empty();});
            std::shared_ptr<T> return_shared_ptr_value(std::make_shared<T>(std::move(priority_data_queue.top())));
            priority_data_queue.pop();
            return return_shared_ptr_value;
        } 

        bool try_pop(T& value)
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            if (priority_data_queue.empty()) {
                return false; 
            }
            value = std::move(priority_data_queue.top());
            priority_data_queue.pop();
            return true;
        }

        std::shared_ptr<T> try_pop()
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            if (priority_data_queue.empty()) {
                return std::shared_ptr<T>();
            }
            std::shared_ptr<T> return_shared_ptr_value(std::make_shared<T>(std::move(priority_data_queue.top())));
            priority_data_queue.pop();
            return return_shared_ptr_value;
        }

        bool empty() const 
        {
            std::lock_guard<std::mutex> lk(this->thread_safe_queue_mutex);
            return priority_data_queue.empty();
        }
};

#endif