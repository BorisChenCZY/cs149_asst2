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


// Helper method implementations
void TaskSystemParallelThreadPoolSleeping::worker_thread_function() {
    while (!terminate) {
        TaskLaunch* task = nullptr;
        bool has_task = false;
        
        // Get task with minimal lock time
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this] { return terminate || !ready_queue.empty(); });
            
            if (!terminate && !ready_queue.empty()) {
                task = ready_queue.front();
                ready_queue.pop();
                has_task = true;
            }
        }
        
        // Execute task
        if (has_task) {
            task->runnable->runTask(task->task_index, task->total_tasks);
            
            // Process completion
            process_task_completion(task->task_id);
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    this->num_threads = num_threads;
    terminate = false;
    // pending_tasks = 0;
    
    // Create worker threads
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread_function, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    // // Wait for all tasks to complete
    // sync(); No need to sync() here, the sync() is called by user
    
    // Signal all threads to stop
    terminate = true;
    queue_cv.notify_all();
    
    // Wait for all threads to finish
    for (auto &t: threads) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // DONE: CS149 students will modify the implementation of this
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
    
    TaskID launch_id = next_launch_id.fetch_add(1);
    
    std::lock_guard<std::mutex> lock(queue_mutex);
    
    // 为每个任务创建TaskInfo对象
    TaskLaunch task = TaskLaunch(launch_id, runnable, 0, num_total_tasks, deps, 0, 0);
    for (int i = 0; i < num_total_tasks; i++) {
        std::vector<TaskID> dependents;  // 暂时为空，后续会被填充
        
        task.dependents.push_back(launch_id);
        // 构建依赖关系：每个依赖都指向当前任务
        for (TaskID dep_id : deps) {
            wait_map[dep_id].dependents.push_back(task_id);
        }
        
        // 无依赖任务直接入ready queue
        if (deps.empty()) {
            ready_queue.push(task);
        }
    }
    
    // 通知工作线程
    queue_cv.notify_all();
    
    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    
    // Wait until all tasks are complete
    std::unique_lock<std::mutex> lock(queue_mutex);
    queue_cv.wait(lock, [this] { return pending_tasks.load() == 0; });
}



void TaskSystemParallelThreadPoolSleeping::process_task_completion(TaskID completed_task_id) {
    std::lock_guard<std::mutex> lock(queue_mutex);
    
    // 标记任务完成
    completed_tasks.insert(completed_task_id);
    pending_tasks--;
    
    // 检查依赖此任务的其他任务
    auto it = wait_map.find(completed_task_id);
    if (it != wait_map.end()) {
        for (TaskID dependent_task_id : it->second) {
            // 检查dependent_task的所有依赖是否都完成
            if (all_dependencies_satisfied(dependent_task_id)) {
                ready_queue.push(all_tasks[dependent_task_id]);
            }
        }
        wait_map.erase(it);  // 清理已处理的依赖
    }
    
    // 通知等待的线程
    queue_cv.notify_all();
}

bool TaskSystemParallelThreadPoolSleeping::all_dependencies_satisfied(TaskID task_id) {
    auto task_it = all_tasks.find(task_id);
    if (task_it == all_tasks.end()) {
        return false;
    }
    
    // 检查该任务的所有依赖是否都已完成
    // 这里需要从wait_map中反向查找该任务的依赖
    for (const auto& pair : wait_map) {
        for (TaskID dependent_id : pair.second) {
            if (dependent_id == task_id) {
                // 如果该任务还在wait_map中，说明还有未完成的依赖
                if (completed_tasks.find(pair.first) == completed_tasks.end()) {
                    return false;
                }
            }
        }
    }
    
    return true;
}
