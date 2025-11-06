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

class thread_pool 
{
    private:
        std::vector<std::thread> thread_vector; 
        uint32_t thread_quantity; 

        mutable std::mutex queue_mutex; 
        std::condition_variable task_condition; 
        std::queue<std::function<void(void)>> task_queue;
        std::atomic<bool> stop = false; 

        void worker_task(void);
        void dequeue_task();

    public: 
        explicit thread_pool(size_t original_vector_size = 1000);

        template<typename T, typename... Args>
        auto enqueue_task(std::function<T(Args...)> desired_task)
        -> std::future<>;

        void enqueue_task(std::function<void()> desired_task);
        void delete_thread(); 
        void add_thread();
        void check_task_queue() const;
        void resize_thread_pool(uint32_t inputed_size);
        thread_pool& operator=(const thread_pool&) = delete; 
        ~thread_pool();
};

// template<typename F, typename... Args>
// auto enqueue_task(F&& f, Args&&... args) 
// -> std::future<typename std::invoke_result<F, Args...>::type>
// {
//     using return_type = typename std::invoke_result<F, Args...>::type;
    
//     auto task = std::make_shared<std::packaged_task<return_type()>>(
//     std::bind(std::forward<F>(f), std::forward<Args>(args)...)
//     );
    
//     std::future<return_type> res = task->get_future();
//     {
//     std::unique_lock<std::mutex> lock(queue_mutex);
//     if(stop) {
//         throw std::runtime_error("enqueue on stopped thread_pool");
//     }
//     task_queue.emplace([task](){ (*task)(); });
//     }
//     task_condition.notify_one();
//     return res;
// }

#endif