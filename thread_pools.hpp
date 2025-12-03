#ifndef THREAD_POOLS_
#define THREAD_POOLS_

#include <vector>
#include <future>
#include <atomic>
#include <iostream>
#include <functional>
#include "work_stealing_queue.hpp"
#include "thread_safe_queue.hpp"

#define INITIAL_THREAD_VECTOR_SIZE 16

class thread_pool
{
    public:
        enum class State {
            RUNNING,    // Normal operation
            SHUTDOWN,   // Reject new tasks, finish pending
            STOPPED     // Reject new tasks, abandon pending
        };

    private:

        std::atomic<size_t> total_tasks_executed{0};
        std::atomic<size_t> total_tasks_submitted{0};
        std::atomic<size_t> total_tasks_stolen{0};
        std::atomic<uint32_t> next_task_sequence_number{0};

        thread_safe_queue<task_attributes_t> priority_work_queue;
        std::vector<std::thread> worker_threads;
        std::mutex pool_mutex;
        std::condition_variable pool_condition_variable;
        std::atomic<bool> stop = false;
        std::vector<std::atomic<bool>> per_thread_retire_flags;

        std::vector<std::unique_ptr<work_stealing_queue<task_attributes_t>>> worker_task_queue;

        static thread_local work_stealing_queue<task_attributes_t> *local_work_stealing_queue;
        static thread_local unsigned local_thread_index;

        // Phase 5: Dynamic scaling configuration
        size_t min_threads;
        size_t max_threads;
        std::atomic<bool> auto_scaling_enabled{false};
        std::thread scaling_monitor_thread;
        std::atomic<State> pool_state{State::RUNNING};
        
        // Thresholds for scaling decisions
        size_t scale_up_threshold = 10;    // Tasks per thread before scaling up
        size_t scale_down_threshold = 2;   // Tasks per thread before scaling down
        std::chrono::milliseconds monitor_interval{1000}; // Check every 1 second
        std::chrono::seconds idle_timeout{5}; // Time before considering scale down
        
        std::chrono::steady_clock::time_point last_busy_time;
        
        void monitor_and_scale();
        size_t get_total_pending_tasks() const;
        size_t get_active_thread_count() const;

        void worker_task(unsigned thread_index);
        bool pop_task_from_local_queue(task_attributes_t& task_attr);
        bool pop_task_from_pool_queue(task_attributes_t& task_attr);
        bool pop_task_from_other_queues(task_attributes_t& task_attr);
        
    public: 
        explicit thread_pool(size_t original_vector_size = 16);

        template<typename FunctionType, typename... Args>
        auto submit_task(priority_t task_priority, FunctionType&& f, Args&&... args)
        -> std::future<typename std::invoke_result<FunctionType, Args...>::type>
        {
            using return_type = typename std::invoke_result<FunctionType, Args...>::type;

            // Check if pool is accepting new tasks
            State current_state = pool_state.load();
            if (current_state != State::RUNNING) {
                // Throw exception or return invalid future
                throw std::runtime_error("Thread pool is shutting down - not accepting new tasks");
            }

            // Create a packaged_task with bound arguments
            auto task_ptr = std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<FunctionType>(f), std::forward<Args>(args)...));

            std::future<return_type> res = task_ptr->get_future();

            task_attributes_t task_attributes;
            task_attributes.task_sequence_number = next_task_sequence_number.fetch_add(1);
            task_attributes.task_priority = task_priority;
            task_attributes.task_function = [task_ptr]() { (*task_ptr)(); };

            if (local_work_stealing_queue) {
                local_work_stealing_queue->push_bottom(std::move(task_attributes));
            } else {
                priority_work_queue.push(std::move(task_attributes));
                // Maybe round-robin to worker queues instead??
                // static std::atomic<size_t> rr_index{0};
                // size_t idx = rr_index.fetch_add(1) % worker_task_queue.size();
                // worker_task_queue[idx]->push_bottom(std::move(task_attributes));
            }

            pool_condition_variable.notify_one();

            return res;
        }

        size_t get_total_steal_count() const
        {
            size_t total_steals = 0;
            for (const auto& queue_ptr : worker_task_queue) {
                total_steals += queue_ptr->get_steal_count();
            }
            return total_steals;
        }

        size_t get_queue_size(unsigned thread_index) const
        {
            if (thread_index < worker_task_queue.size()) {
                return worker_task_queue[thread_index]->size();
            }
            return 0;
        }

        size_t get_total_tasks_submitted() const {
            return next_task_sequence_number.load();
        }

        size_t get_total_tasks_executed() const {
            return get_total_tasks_submitted() - priority_work_queue.size();
        }

        void add_workers(size_t num_new_workers);
        void remove_workers(size_t num_workers_to_remove);
        void resize_pool(size_t new_size);

        void enable_auto_scaling(size_t min_threads, size_t max_threads);
        void disable_auto_scaling();
        void set_scale_thresholds(size_t up_threshold, size_t down_threshold);

        void shutdown(bool wait_for_pending = true);
        State get_state() const { return pool_state.load(); }
        bool is_running() const { return pool_state.load() == State::RUNNING; }

        thread_pool(const thread_pool&) = delete;
        thread_pool& operator=(const thread_pool&) = delete;
        ~thread_pool();
};

#endif