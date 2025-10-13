#include "tasksys.h"
#include <iostream>
#include <cassert>
#include <atomic>
#include <unistd.h>
#include <chrono>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), m_max_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    // Create worker threads
    for (int i = 0; i < num_threads; i++) {
        m_threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread_function, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    // Signal all threads to stop
    m_done = true;
    m_task_cv.notify_all();
    
    // Wait for all threads to finish
    for (auto &t: m_threads) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // Use runAsyncWithDeps with no dependencies, then sync
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Generate unique TaskID for this launch
    TaskID launch_id = m_next_task_id.fetch_add(1);
    
    // Create TaskLaunch object
    auto launch = std::make_shared<TaskLaunch>();
    launch->id = launch_id;
    launch->runnable = runnable;
    launch->num_total_tasks = num_total_tasks;
    launch->dependencies = deps;
    
    // Create individual tasks
    for (int i = 0; i < num_total_tasks; i++) {
        Task task;
        task.runnable = runnable;
        task.task_id = i;
        task.num_total_tasks = num_total_tasks;
        task.launch_id = launch_id;
        launch->tasks.push_back(task);
    }
    
    // Store the launch
    {
        std::lock_guard<std::mutex> lock(m_task_mutex);
        m_task_launches[launch_id] = launch;
        m_pending_launches++;
        
        // Check if dependencies are satisfied
        if (are_dependencies_satisfied(deps)) {
            // All dependencies satisfied, add tasks to ready queue
            for (const auto& task : launch->tasks) {
                m_ready_queue.push(task);
            }
            m_task_cv.notify_all();
        } else {
            // Dependencies not satisfied, register dependents
            for (TaskID dep_id : deps) {
                m_dependents[dep_id].insert(launch_id);
            }
        }
    }

    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // Wait until all pending launches are complete
    std::unique_lock<std::mutex> lock(m_completed_mutex);
    m_completed_cv.wait(lock, [this] { return m_pending_launches.load() == 0; });
}

// Helper method implementations
bool TaskSystemParallelThreadPoolSleeping::are_dependencies_satisfied(const std::vector<TaskID>& deps) {
    for (TaskID dep_id : deps) {
        auto it = m_task_launches.find(dep_id);
        if (it == m_task_launches.end() || !it->second->is_complete.load()) {
            return false;
        }
    }
    return true;
}

void TaskSystemParallelThreadPoolSleeping::move_ready_tasks_to_queue(TaskID completed_launch_id) {
    // Find all launches that depend on this completed launch
    auto dependents_it = m_dependents.find(completed_launch_id);
    if (dependents_it != m_dependents.end()) {
        for (TaskID dependent_id : dependents_it->second) {
            auto launch_it = m_task_launches.find(dependent_id);
            if (launch_it != m_task_launches.end()) {
                auto launch = launch_it->second;
                // Check if all dependencies are now satisfied
                if (are_dependencies_satisfied(launch->dependencies)) {
                    // Add all tasks from this launch to ready queue
                    for (const auto& task : launch->tasks) {
                        m_ready_queue.push(task);
                    }
                }
            }
        }
        // Clean up dependents map
        m_dependents.erase(dependents_it);
    }
    
    // Notify worker threads that new tasks are available
    m_task_cv.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::worker_thread_function() {
    while (!m_done) {
        Task task;
        bool has_task = false;
        
        // Try to get a task from ready queue
        {
            std::unique_lock<std::mutex> lock(m_task_mutex);
            m_task_cv.wait(lock, [this] { return m_done || !m_ready_queue.empty(); });
            
            if (!m_done && !m_ready_queue.empty()) {
                task = m_ready_queue.front();
                m_ready_queue.pop();
                has_task = true;
            }
        }
        
        if (has_task) {
            // Execute the task
            task.runnable->runTask(task.task_id, task.num_total_tasks);
            
            // Update completion status
            {
                std::lock_guard<std::mutex> lock(m_task_mutex);
                auto launch_it = m_task_launches.find(task.launch_id);
                if (launch_it != m_task_launches.end()) {
                    auto launch = launch_it->second;
                    int completed = launch->completed_tasks.fetch_add(1) + 1;
                    
                    // Check if this launch is now complete
                    if (completed == launch->num_total_tasks) {
                        launch->is_complete.store(true);
                        
                        // Move dependent tasks to ready queue
                        move_ready_tasks_to_queue(task.launch_id);
                        
                        // Decrease pending launches count
                        int pending = m_pending_launches.fetch_sub(1) - 1;
                        
                        // Notify sync() if all launches are complete
                        if (pending == 0) {
                            m_completed_cv.notify_all();
                        }
                    }
                }
            }
        }
    }
}
