#include <iostream>
#include <vector>
#include <thread>
#include <numeric> // For std::iota

void worker_function(int id) {
    std::cout << "Thread " << id << " is running." << std::endl;
    // Simulate some work
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "Thread " << id << " finished." << std::endl;
}

int main() {
    std::vector<std::thread> threads;
    int num_threads = 5;

    // Create and add threads to the vector
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker_function, i);
    }

    std::cout << "All threads launched." << std::endl;

    // Join all threads to wait for their completion
    for (std::thread& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    std::cout << "All threads joined." << std::endl;

    return 0;
}