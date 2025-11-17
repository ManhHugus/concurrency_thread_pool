#ifndef WORK_STEALING_QUEUE_
#define WORK_STEALING_QUEUE_
#include <queue>
#include <deque>

template <typename T>
class work_stealing_queue 
{
    private:
        // Add member variables and methods for work stealing queue
        std::deque<T> local_working_queue;
        mutable std::mutex local_queue_mutex;

    public:
        work_stealing_queue() {}
        ~work_stealing_queue() {}

        work_stealing_queue& operator=(const work_stealing_queue&) = delete;
        work_stealing_queue(const work_stealing_queue&) = delete;

        void push_bottom(const T& item) 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            local_working_queue.push_back(std::move(item));
        }

        void pop_bottom(T& item) 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            if (!local_working_queue.empty()) {
                item = std::move(local_working_queue.back());
                local_working_queue.pop_back();
            }
        }

        void steal_top(T& item) 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            if (!local_working_queue.empty()) {
                item = std::move(local_working_queue.front());
                local_working_queue.pop_front();
            }
        }

        bool empty() const 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            return local_working_queue.empty();
        }
};


#endif