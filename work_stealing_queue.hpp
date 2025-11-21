#ifndef WORK_STEALING_QUEUE_
#define WORK_STEALING_QUEUE_
#include <deque>

template <typename T>
class work_stealing_queue 
{
    private:
        // Add member variables and methods for work stealing queue
        std::deque<T> local_working_queue;
        mutable std::mutex local_queue_mutex;
        std::atomic<size_t> steal_count{0};

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

        bool pop_bottom(T& item)
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            if (!local_working_queue.empty()) {
                item = std::move(local_working_queue.back());
                local_working_queue.pop_back();
                return true;
            }
            return false;
        }

        bool steal_top(T& item) 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            if (!local_working_queue.empty()) {
                item = std::move(local_working_queue.front());
                local_working_queue.pop_front();
                steal_count.fetch_add(1);
                return true;
            }
            return false;
        }

        bool empty() const 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            return local_working_queue.empty();
        }

        size_t size() const 
        {
            std::lock_guard<std::mutex> lock(local_queue_mutex);
            return local_working_queue.size();
        }

        size_t get_steal_count() const 
        { 
            return steal_count.load();
        }
};


#endif