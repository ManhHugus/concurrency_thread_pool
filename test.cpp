// #include <iostream>
// #include <vector>
// #include <thread>
// #include <assert.h>
// #include <chrono>
// #include <atomic>
// #include <string>
// #include <numeric> // For std::iota

#include <iostream>
#include <future>
#include <thread>
#include <chrono>

// void worker_function(int id) {
//     std::cout << "Thread " << id << " is running." << std::endl;
//     // Simulate some work
//     std::this_thread::sleep_for(std::chrono::milliseconds(100));
//     std::cout << "Thread " << id << " finished." << std::endl;
// }

// struct X
// {
//     int i;
//     std::string s;
// };
// std::atomic<X*> p;
// std::atomic<int> a;

// void create_x()
// {
//     X* x=new X;
//     x->i=42;
//     x->s="hello";
//     a.store(99,std::memory_order_relaxed);
//     p.store(x,std::memory_order_release);
// }

// void use_x()
// {
//     X* x;
//     while(!(x=p.load(std::memory_order_consume)))
//     std::this_thread::sleep_for(std::chrono::microseconds(1));
//     assert(x->i==42);
//     assert(x->s=="hello");
//     assert(a.load(std::memory_order_relaxed)==99);
// }
// int main()
// {
//     std::thread t1(create_x);
//     std::thread t2(use_x);
//     t1.join();
//     t2.join();
// }

// int main() {
//     std::vector<std::thread> threads;
//     int num_threads = 5;

//     // Create and add threads to the vector
//     for (int i = 0; i < num_threads; ++i) {
//         threads.emplace_back(worker_function, i);
//     }

//     std::cout << "All threads launched." << std::endl;

//     // Join all threads to wait for their completion
//     for (std::thread& t : threads) {
//         if (t.joinable()) {
//             t.join();
//         }
//     }

//     std::cout << "All threads joined." << std::endl;

//     return 0;
// }

template<typename F, typename... Args>
auto enqueue_task(F&& f, Args&&... args) 
-> std::future<typename std::invoke_result<F, Args...>::type>
{
    using return_type = typename std::invoke_result<F, Args...>::type;
    
    auto task = std::make_shared<std::packaged_task<return_type()>>(
    std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );
    
    std::future<return_type> res = task->get_future();
    {
    std::unique_lock<std::mutex> lock(queue_mutex);
    if(stop) {
        throw std::runtime_error("enqueue on stopped thread_pool");
    }
    task_queue.emplace([task](){ (*task)(); });
    }
    task_condition.notify_one();
    return res;
}

int calculate_sum(int a, int b) {
    std::this_thread::sleep_for(std::chrono::seconds(1)); // Simulate work
    return a + b;
}

int main() {
    // Create a packaged_task wrapping the calculate_sum function
    std::packaged_task<int(int, int)> task(calculate_sum);

    // Get the future associated with the task
    std::future<int> future_result = task.get_future();

    // Launch a new thread to execute the packaged_task
    std::thread worker_thread(std::move(task), 5, 7);

    // Do other work in the main thread while the task runs
    std::cout << "Main thread doing other work...\n";

    // Wait for the result and retrieve it
    int result = future_result.get();
    std::cout << "Result from packaged_task: " << result << std::endl;

    // Join the worker thread
    worker_thread.join();

    return 0;
}