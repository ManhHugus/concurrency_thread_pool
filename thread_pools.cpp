#include "thread_pools.hpp"

thread_pool::thread_pool(size_t original_vector_size) : stop(false), min_threads(original_vector_size), max_threads(original_vector_size * 4)
{
    try
    {
        for (unsigned index = 0; index < original_vector_size; index++) {
            worker_task_queue.push_back(std::make_unique<work_stealing_queue<task_attributes_t>>());
            worker_threads.push_back(std::thread(&thread_pool::worker_task, this, index));
            per_thread_retire_flags.push_back(false);
        }
    }
    catch(...)
    {
        stop.store(true, std::memory_order_relaxed);
        throw;
    }
}

void thread_pool::worker_task(unsigned thread_index)
{
    local_thread_index = thread_index;
    local_work_stealing_queue = worker_task_queue[thread_index].get();
    task_attributes_t task_attr;
    
    while (!stop.load() && !per_thread_retire_flags[thread_index].load()) {
        if (pop_task_from_local_queue(task_attr) ||
            pop_task_from_pool_queue(task_attr) ||
            pop_task_from_other_queues(task_attr)) {
            try {
                task_attr.task_function();
            }
            catch(const std::exception& e) {
                std::cerr << "[Worker " << thread_index << "] Exception: " << e.what() << '\n';
            }
        } else {
            // Check if we should exit due to shutdown
            State current_state = pool_state.load();
            if (current_state == State::STOPPED) {
                // Immediate stop - exit even if there are pending tasks
                break;
            }
            
            std::unique_lock<std::mutex> lock(pool_mutex);
            pool_condition_variable.wait(lock, [this, thread_index]{ 
                return stop.load() || 
                       per_thread_retire_flags[thread_index].load() ||
                       !priority_work_queue.empty() || 
                       !local_work_stealing_queue->empty(); 
            });
        }
    }
    
    std::cout << "[Worker " << thread_index << "] Thread exiting.\n";
}

bool thread_pool::pop_task_from_local_queue(task_attributes_t& task_attr)
{
    return (local_work_stealing_queue && !local_work_stealing_queue->empty()) ? 
        local_work_stealing_queue->pop_bottom(task_attr), true : false;
}

bool thread_pool::pop_task_from_pool_queue(task_attributes_t& task_attr)
{
    return priority_work_queue.try_pop(task_attr);
}

bool thread_pool::pop_task_from_other_queues(task_attributes_t& task_attr)
{
    for (size_t i = 0; i < worker_task_queue.size(); i++) {
        unsigned const index = (local_thread_index + i + 1) % worker_task_queue.size(); // Avoid stealing from the first thread

        if (index == local_thread_index) {
            continue;
        }

        if (!worker_task_queue[index]->empty()) {
            worker_task_queue[index]->steal_top(task_attr);
            return true;
        }
    }
    return false;
}

void thread_pool::add_workers(size_t num_new_workers)
{
    // Implementation for adding workers
    for (int i = 0; i < num_new_workers; i++) {
        size_t new_thread_index = worker_threads.size();
        worker_task_queue.push_back(std::make_unique<work_stealing_queue<task_attributes_t>>());
        worker_threads.push_back(std::thread(&thread_pool::worker_task, this, new_thread_index));
        per_thread_retire_flags.push_back(false);
    }
}

// void thread_pool::remove_workers(size_t workers_index_to_be_removed)
// {
//     // Move any remaining tasks from this worker's queue to the global queue
//     task_attributes_t task_attr;
//     while (worker_task_queue[workers_index_to_be_removed]->pop_bottom(task_attr)) {
//         priority_work_queue.push(std::move(task_attr));
//     }
    
//     {
//         std::unique_lock<std::mutex> lock(pool_mutex);
//         per_thread_retire_flags[workers_index_to_be_removed].store(true);
//         pool_condition_variable.notify_all();
//     }

//     if (worker_threads[workers_index_to_be_removed].joinable()) {
//         worker_threads[workers_index_to_be_removed].join();
//     }
    
//     worker_threads.erase(worker_threads.begin() + workers_index_to_be_removed);
//     worker_task_queue.erase(worker_task_queue.begin() + workers_index_to_be_removed);
//     per_thread_retire_flags.erase(per_thread_retire_flags.begin() + workers_index_to_be_removed);
// }

void thread_pool::remove_workers(size_t num_workers_to_remove)
{
    std::lock_guard<std::mutex> lock(pool_mutex);
    
    for (size_t i = 0; i < num_workers_to_remove && !worker_threads.empty(); ++i) {
        size_t last_index = worker_threads.size() - 1;
        
        // Signal the last thread to retire
        per_thread_retire_flags[last_index].store(true);
        pool_condition_variable.notify_all();
        
        // Unlock to allow thread to exit, then rejoin
        {
            std::unique_lock<std::mutex> temp_lock(pool_mutex);
            temp_lock.unlock();
            
            if (worker_threads[last_index].joinable()) {
                worker_threads[last_index].join();
            }
            
            temp_lock.lock();
        }
        
        // Remove from vectors
        worker_threads.pop_back();
        worker_task_queue.pop_back();
        per_thread_retire_flags.pop_back();
    }
}

void thread_pool::resize_pool(size_t new_size)
{
    std::unique_lock<std::mutex> lock(pool_mutex);
    size_t current_size = worker_threads.size();
    if (new_size > current_size) {
        add_workers(new_size - current_size);
    } else if (new_size < current_size) {
        // Remove from the end for simplicity
        for (size_t i = current_size; i > new_size; --i) {
            remove_workers(i - 1);
        }
    }
}

void thread_pool::enable_auto_scaling(size_t min, size_t max)
{
    if (min < 1) min = 1;
    if (max < min) max = min;
    
    min_threads = min;
    max_threads = max;
    auto_scaling_enabled.store(true);
    last_busy_time = std::chrono::steady_clock::now();
    
    scaling_monitor_thread = std::thread(&thread_pool::monitor_and_scale, this);
}

void thread_pool::disable_auto_scaling()
{
    auto_scaling_enabled.store(false);
    if (scaling_monitor_thread.joinable()) {
        scaling_monitor_thread.join();
    }
}

void thread_pool::set_scale_thresholds(size_t up_threshold, size_t down_threshold)
{
    scale_up_threshold = up_threshold;
    scale_down_threshold = down_threshold;
}

size_t thread_pool::get_total_pending_tasks() const
{
    size_t total = priority_work_queue.size();
    for (const auto& queue_ptr : worker_task_queue) {
        total += queue_ptr->size();
    }
    return total;
}

size_t thread_pool::get_active_thread_count() const
{
    std::lock_guard<std::mutex> lock(pool_mutex);
    return worker_threads.size();
}

void thread_pool::monitor_and_scale()
{
    while (auto_scaling_enabled.load() && !stop.load()) {
        std::this_thread::sleep_for(monitor_interval);
        
        if (stop.load()) break;
        
        size_t pending_tasks = get_total_pending_tasks();
        size_t current_threads = get_active_thread_count();
        
        if (current_threads == 0) continue;
        
        size_t tasks_per_thread = pending_tasks / current_threads;
        
        // Scale up if overloaded
        if (tasks_per_thread >= scale_up_threshold && current_threads < max_threads) {
            // Calculate how many threads to add (at least 1, at most double current or reach max)
            size_t threads_to_add = std::min({
                current_threads,  // Double the threads
                max_threads - current_threads,  // Don't exceed max
                (pending_tasks / scale_up_threshold) - current_threads  // Based on workload
            });
            
            if (threads_to_add > 0) {
                std::cout << "[Auto-Scale] Adding " << threads_to_add 
                          << " threads (load: " << tasks_per_thread << " tasks/thread)\n";
                resize_pool(current_threads + threads_to_add);
                last_busy_time = std::chrono::steady_clock::now();
            }
        }
        // Scale down if underutilized
        else if (tasks_per_thread <= scale_down_threshold && current_threads > min_threads) {
            auto now = std::chrono::steady_clock::now();
            auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_busy_time);
            
            // Only scale down after being idle for a while (avoid thrashing)
            if (idle_duration >= idle_timeout) {
                // Remove at most half the threads or down to min_threads
                size_t threads_to_remove = std::min({
                    current_threads / 2,
                    current_threads - min_threads,
                    size_t(1)  // Remove at least 1 at a time for gradual scaling
                });
                
                if (threads_to_remove > 0) {
                    std::cout << "[Auto-Scale] Removing " << threads_to_remove 
                              << " threads (load: " << tasks_per_thread << " tasks/thread)\n";
                    resize_pool(current_threads - threads_to_remove);
                }
            }
        }
        else {
            // Update last busy time if we have significant work
            if (pending_tasks > 0) {
                last_busy_time = std::chrono::steady_clock::now();
            }
        }
    }
}

void thread_pool::shutdown(bool wait_for_pending)
{
    // Already shutting down or stopped
    State expected = State::RUNNING;
    State desired = wait_for_pending ? State::SHUTDOWN : State::STOPPED;
    
    if (!pool_state.compare_exchange_strong(expected, desired)) {
        // Already in shutdown or stopped state
        return;
    }

    std::cout << "[Shutdown] Initiating " 
              << (wait_for_pending ? "graceful" : "immediate") 
              << " shutdown...\n";

    // Stop auto-scaling if enabled
    disable_auto_scaling();

    if (wait_for_pending) {
        // SHUTDOWN mode: Wait for all pending tasks to complete
        std::cout << "[Shutdown] Waiting for " << get_total_pending_tasks() 
                  << " pending tasks to complete...\n";
        
        // Wait until all queues are empty
        while (get_total_pending_tasks() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "[Shutdown] All pending tasks completed.\n";
    } else {
        // STOPPED mode: Abandon pending tasks
        size_t abandoned_tasks = get_total_pending_tasks();
        std::cout << "[Shutdown] Abandoning " << abandoned_tasks << " pending tasks.\n";
    }

    // Signal all threads to stop
    stop.store(true, std::memory_order_release);
    
    // Wake up all waiting threads
    priority_work_queue.notify_all_data_condition();
    pool_condition_variable.notify_all();
    
    // Join all worker threads
    std::cout << "[Shutdown] Joining " << worker_threads.size() << " worker threads...\n";
    for (size_t index = 0; index < worker_threads.size(); index++) {
        if (worker_threads[index].joinable()) {
            worker_threads[index].join();
        }
    }
    
    std::cout << "[Shutdown] Thread pool stopped.\n";
}

thread_pool::~thread_pool()
{
    // Call graceful shutdown by default
    if (pool_state.load() == State::RUNNING) {
        std::cout << "[Destructor] Performing graceful shutdown...\n";
        shutdown(true);  // Wait for pending tasks
    }
}