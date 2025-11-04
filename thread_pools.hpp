#ifndef THREAD_POOLS_
#define THREAD_POOLS_

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <iostream>

#define INITIAL_THREAD_VECTOR_SIZE 1000

typedef struct {
    std::thread thread_main;
    uint32_t thread_id;
}thread_info_t;

// Core Funtional Requirements
// Fixed or Configurable Number of Threads
// Allow specifying the number of worker threads at construction.

// Task Submission Interface
// Support submitting arbitrary callable objects (lambdas, functions, functors).
// Return a std::future for result retrieval.

// Thread-Safe Task Queue
// Ensure multiple threads can safely push and pop tasks concurrently.

// Worker Thread Loop
// Threads should continuously wait for tasks and execute them when available.

class thread_pool 
{
    private:
        std::vector<thread_info_t> thread_info_vector;
        std::vector<std::thread> thread_vector; 
        uint32_t thread_quantity; 

        std::mutex queue_mutex; 
        std::condition_variable task_condition; 
        std::queue<std::function<void(void)>> task_queue;
        std::atomic<bool> stop = false; 

        void worker_task(void);

    public: 
        explicit thread_pool(size_t original_vector_size = 1000);
        void enqueue_task(std::function<void()> desired_task);
        void dequeue_task(); 

        void delete_thread(uint32_t thread_id);
        void check_task_queue();
        void resize_thread_pool(uint32_t inputed_size);
        thread_pool& operator=(const thread_pool&) = delete; 
        ~thread_pool();
};

#endif