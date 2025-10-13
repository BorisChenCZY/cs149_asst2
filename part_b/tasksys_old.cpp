#include "tasksys.h"
#include <iostream>
#include <cassert>
#include <atomic>
#ifdef _WIN32
// no unistd.h on Windows
#else
#include <unistd.h>
#endif
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
    launch->remainingDeps.store((int)deps.size());
    
    // Store the launch first
    {
        std::lock_guard<std::mutex> lock(m_task_mutex);
        m_task_launches[launch_id] = launch;
        m_pending_launches++;

        // Register dependents on existing deps
        for (TaskID dep_id : deps) {
            auto it = m_task_launches.find(dep_id);
            if (it != m_task_launches.end()) {
                it->second->dependents.push_back(launch_id);
            } else {
                m_dependents[dep_id].insert(launch_id);
            }
        }

        // If no deps, enqueue tasks immediately
        if (launch->remainingDeps.load() == 0) {
            for (int i = 0; i < num_total_tasks; i++) {
                m_ready_queue.push(Task{runnable, i, num_total_tasks, launch_id});
            }
            m_task_cv.notify_all();
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
    // Primary: use dependents vector on completed launch
    auto itCompleted = m_task_launches.find(completed_launch_id);
    if (itCompleted != m_task_launches.end()) {
        for (TaskID child : itCompleted->second->dependents) {
            auto itChild = m_task_launches.find(child);
            if (itChild != m_task_launches.end()) {
                int left = itChild->second->remainingDeps.fetch_sub(1) - 1;
                if (left == 0) {
                    for (int i = 0; i < itChild->second->num_total_tasks; i++) {
                        m_ready_queue.push(Task{itChild->second->runnable, i, itChild->second->num_total_tasks, itChild->second->id});
                    }
                }
            }
        }
    }
    // Legacy: handle pending dependents registered before dep was created
    auto dependents_it = m_dependents.find(completed_launch_id);
    if (dependents_it != m_dependents.end()) {
        for (TaskID child : dependents_it->second) {
            auto itChild = m_task_launches.find(child);
            if (itChild != m_task_launches.end()) {
                int left = itChild->second->remainingDeps.fetch_sub(1) - 1;
                if (left == 0) {
                    for (int i = 0; i < itChild->second->num_total_tasks; i++) {
                        m_ready_queue.push(Task{itChild->second->runnable, i, itChild->second->num_total_tasks, itChild->second->id});
                    }
                }
            }
        }
        m_dependents.erase(dependents_it);
    }
    m_task_cv.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::worker_thread_function() {
    while (!m_done) {
        Task task;
        bool has_task = false;
        
        // Get task with minimal lock time
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
            // Execute task outside of lock
            task.runnable->runTask(task.task_id, task.num_total_tasks);
            
            // Minimal lock time for completion tracking
            bool should_notify_sync = false;
            {
                std::lock_guard<std::mutex> lock(m_task_mutex);
                auto launch_it = m_task_launches.find(task.launch_id);
                if (launch_it != m_task_launches.end()) {
                    auto launch = launch_it->second;
                    int completed = launch->completed_tasks.fetch_add(1) + 1;
                    
                    if (completed == launch->num_total_tasks) {
                        launch->is_complete.store(true);
                        move_ready_tasks_to_queue(task.launch_id);
                        
                        int pending = m_pending_launches.fetch_sub(1) - 1;
                        should_notify_sync = (pending == 0);
                    }
                }
            }
            
            // Notify outside of lock
            if (should_notify_sync) {
                m_completed_cv.notify_all();
            }
        }
    }
}
