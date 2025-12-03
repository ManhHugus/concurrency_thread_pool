#include "thread_pools.hpp"
#include <iostream>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>

void safe_print(const std::string& message) {
    static std::mutex print_mutex;
    std::lock_guard<std::mutex> lock(print_mutex);
    std::cout << message << std::endl;
}

// Test 1: Basic task submission with different priorities
void test_basic_priority() {
    std::cout << "\n=== Test 1: Basic Priority Task Submission ===\n";
    thread_pool pool(4);
    
    std::vector<std::future<int>> results;
    
    // Submit tasks with different priorities
    for (int i = 0; i < 10; i++) {
        priority_t priority = (i % 3 == 0) ? HIGH_PRIORITY : 
                             (i % 3 == 1) ? MEDIUM_PRIORITY : LOW_PRIORITY;
        
        results.push_back(pool.submit_task(priority, [i, priority]() -> int {
            std::stringstream ss;
            ss << "Task " << i << " (Priority: " << priority << ") executing";
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return i * 2;
        }));
    }
    
    // Wait for all results
    std::cout << "\nResults: ";
    for (auto& fut : results) {
        std::cout << fut.get() << " ";
    }
    std::cout << "\n";
}

// Test 2: Return values and futures
void test_return_values() {
    std::cout << "\n=== Test 2: Task Return Values ===\n";
    thread_pool pool(4);
    
    // Test different return types
    auto int_result = pool.submit_task(MEDIUM_PRIORITY, []() -> int { 
        return 42; 
    });
    
    auto string_result = pool.submit_task(MEDIUM_PRIORITY, []() -> std::string { 
        return "Hello from thread pool!"; 
    });
    
    auto double_result = pool.submit_task(MEDIUM_PRIORITY, [](double a, double b) -> double { 
        return a * b; 
    }, 3.14, 2.0);
    
    std::cout << "Int result: " << int_result.get() << "\n";
    std::cout << "String result: " << string_result.get() << "\n";
    std::cout << "Double result: " << double_result.get() << "\n";
}

// Test 3: Exception handling
void test_exception_handling() {
    std::cout << "\n=== Test 3: Exception Handling ===\n";
    thread_pool pool(4);
    
    auto safe_task = pool.submit_task(MEDIUM_PRIORITY, []() -> int {
        safe_print("Safe task executing normally");
        return 100;
    });
    
    auto throwing_task = pool.submit_task(MEDIUM_PRIORITY, []() -> int {
        safe_print("Task about to throw exception");
        throw std::runtime_error("Intentional error!");
        return 200;
    });
    
    // Get safe result
    std::cout << "Safe task result: " << safe_task.get() << "\n";
    
    // Try to get throwing result
    try {
        throwing_task.get();
    } catch (const std::exception& e) {
        std::cout << "Caught exception from task: " << e.what() << "\n";
    }
}

// Test 4: Work stealing
void test_work_stealing() {
    std::cout << "\n=== Test 4: Work Stealing ===\n";
    thread_pool pool(4);
    
    std::cout << "Initial steal count: " << pool.get_total_steal_count() << "\n";
    
    // Submit many tasks from the main thread (external submission)
    // These go to the global queue
    for (int i = 0; i < 20; i++) {
        pool.submit_task(MEDIUM_PRIORITY, [i]() {
            std::stringstream ss;
            ss << "External task " << i << " executing";
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    }
    
    // Submit tasks from within worker threads (local submission)
    auto nested_task = pool.submit_task(HIGH_PRIORITY, [&pool]() {
        safe_print("Worker thread submitting nested tasks");
        for (int i = 0; i < 10; i++) {
            pool.submit_task(MEDIUM_PRIORITY, [i]() {
                std::stringstream ss;
                ss << "  Nested task " << i << " executing";
                safe_print(ss.str());
                std::this_thread::sleep_for(std::chrono::milliseconds(30));
            });
        }
    });
    
    nested_task.wait();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    std::cout << "\nFinal steal count: " << pool.get_total_steal_count() << "\n";
    std::cout << "Total tasks submitted: " << pool.get_total_tasks_submitted() << "\n";
    std::cout << "Total tasks executed: " << pool.get_total_tasks_executed() << "\n";
}

// Test 5: Dynamic scaling
void test_dynamic_scaling() {
    std::cout << "\n=== Test 5: Dynamic Thread Scaling ===\n";
    thread_pool pool(2);  // Start with 2 threads
    
    // Enable auto-scaling: min 2, max 8 threads
    pool.enable_auto_scaling(2, 8);
    pool.set_scale_thresholds(5, 1);  // Scale up at 5 tasks/thread, down at 1 task/thread
    
    std::cout << "Starting with 2 threads, auto-scaling enabled (min: 2, max: 8)\n";
    
    // Phase 1: Submit many tasks to trigger scale-up
    std::cout << "\nPhase 1: Submitting 50 tasks to trigger scale-up...\n";
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 50; i++) {
        futures.push_back(pool.submit_task(MEDIUM_PRIORITY, [i]() {
            std::stringstream ss;
            ss << "Heavy load task " << i;
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }));
    }
    
    // Give time for scaling
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "Active threads after scale-up: " << pool.get_active_thread_count() << "\n";
    
    // Wait for tasks to complete
    for (auto& f : futures) {
        f.wait();
    }
    
    std::cout << "\nPhase 2: Waiting for idle timeout to trigger scale-down...\n";
    std::this_thread::sleep_for(std::chrono::seconds(6));
    std::cout << "Active threads after scale-down: " << pool.get_active_thread_count() << "\n";
    
    pool.disable_auto_scaling();
}

// Test 6: Manual resizing
void test_manual_resize() {
    std::cout << "\n=== Test 6: Manual Pool Resizing ===\n";
    thread_pool pool(4);
    
    std::cout << "Initial thread count: " << pool.get_active_thread_count() << "\n";
    
    // Manually grow the pool
    std::cout << "Growing pool to 8 threads...\n";
    pool.resize_pool(8);
    std::cout << "Thread count after growth: " << pool.get_active_thread_count() << "\n";
    
    // Submit some work
    for (int i = 0; i < 16; i++) {
        pool.submit_task(MEDIUM_PRIORITY, [i]() {
            std::stringstream ss;
            ss << "Task " << i << " on expanded pool";
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        });
    }
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    // Manually shrink the pool
    std::cout << "\nShrinking pool to 3 threads...\n";
    pool.resize_pool(3);
    std::cout << "Thread count after shrink: " << pool.get_active_thread_count() << "\n";
}

// Test 7: Graceful shutdown
void test_graceful_shutdown() {
    std::cout << "\n=== Test 7: Graceful Shutdown ===\n";
    thread_pool pool(4);
    
    std::atomic<int> completed_tasks{0};
    
    // Submit long-running tasks
    for (int i = 0; i < 10; i++) {
        pool.submit_task(MEDIUM_PRIORITY, [i, &completed_tasks]() {
            std::stringstream ss;
            ss << "Long task " << i << " starting";
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            completed_tasks++;
            ss.str("");
            ss << "Long task " << i << " completed";
            safe_print(ss.str());
        });
    }
    
    std::cout << "Initiating graceful shutdown (wait for pending tasks)...\n";
    pool.shutdown(true);
    
    std::cout << "Completed tasks: " << completed_tasks << " / 10\n";
}

// Test 8: Immediate shutdown
void test_immediate_shutdown() {
    std::cout << "\n=== Test 8: Immediate Shutdown ===\n";
    thread_pool* pool_ptr = new thread_pool(4);
    
    std::atomic<int> completed_tasks{0};
    
    for (int i = 0; i < 20; i++) {
        pool_ptr->submit_task(MEDIUM_PRIORITY, [i, &completed_tasks]() {
            std::stringstream ss;
            ss << "Task " << i << " executing";
            safe_print(ss.str());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            completed_tasks++;
        });
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "Initiating immediate shutdown (abandon pending tasks)...\n";
    pool_ptr->shutdown(false);
    
    std::cout << "Completed tasks before shutdown: " << completed_tasks << " / 20\n";
    delete pool_ptr;
}

// Test 9: State checking and rejection during shutdown
void test_shutdown_rejection() {
    std::cout << "\n=== Test 9: Task Rejection During Shutdown ===\n";
    thread_pool pool(4);
    
    for (int i = 0; i < 5; i++) {
        pool.submit_task(MEDIUM_PRIORITY, [i]() {
            safe_print("Initial task " + std::to_string(i));
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        });
    }
    
    std::thread shutdown_thread([&pool]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        pool.shutdown(true);
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    
    try {
        pool.submit_task(MEDIUM_PRIORITY, []() {
            safe_print("This should not execute!");
        });
        std::cout << "ERROR: Task was accepted during shutdown!\n";
    } catch (const std::runtime_error& e) {
        std::cout << "Correctly rejected task: " << e.what() << "\n";
    }
    
    shutdown_thread.join();
}

// Test 10: Stress test
void test_stress() {
    std::cout << "\n=== Test 10: Stress Test ===\n";
    thread_pool pool(8);
    pool.enable_auto_scaling(4, 16);
    
    const int NUM_TASKS = 1000;
    std::atomic<int> completed{0};
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> duration_dist(1, 50);
    std::uniform_int_distribution<> priority_dist(0, 2);
    
    auto start_time = std::chrono::steady_clock::now();
    
    for (int i = 0; i < NUM_TASKS; i++) {
        priority_t priority = static_cast<priority_t>(priority_dist(gen));
        int sleep_ms = duration_dist(gen);
        
        pool.submit_task(priority, [i, sleep_ms, &completed]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            completed++;
            if (i % 100 == 0) {
                std::stringstream ss;
                ss << "Completed " << completed << " / " << NUM_TASKS << " tasks";
                safe_print(ss.str());
            }
        });
    }
    
    while (completed < NUM_TASKS) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "\nStress Test Results:\n";
    std::cout << "  Tasks completed: " << completed << " / " << NUM_TASKS << "\n";
    std::cout << "  Total time: " << duration.count() << " ms\n";
    std::cout << "  Total steals: " << pool.get_total_steal_count() << "\n";
    std::cout << "  Peak threads: " << pool.get_active_thread_count() << "\n";
    
    pool.disable_auto_scaling();
}

int main() {
    std::cout << "==================================================\n";
    std::cout << "    Advanced Thread Pool Test Suite\n";
    std::cout << "==================================================\n";
    
    try {
        test_basic_priority();
        test_return_values();
        test_exception_handling();
        test_work_stealing();
        test_dynamic_scaling();
        test_manual_resize();
        test_graceful_shutdown();
        test_immediate_shutdown();
        test_shutdown_rejection();
        test_stress();
        
        std::cout << "\n==================================================\n";
        std::cout << "    All Tests Completed Successfully!\n";
        std::cout << "==================================================\n";
        
    } catch (const std::exception& e) {
        std::cerr << "\nTest failed with exception: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}