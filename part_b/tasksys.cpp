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
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait(lock, [this] { return terminate || !ready_queue.empty(); });
        
        // ! BUG 从ready queue里面删除launch的时机不好把控，此处有bug - chenhzhu 10/14/2025
        auto task_iter = std::find_if(ready_queue.begin(), ready_queue.end(), [](TaskLaunch* t) { return t->curr_task_id < t->total_tasks; });
        if (!terminate && task_iter != ready_queue.end()) {
        // if (!terminate && !ready_queue.empty()) {
            
            task = *task_iter;
            // task = ready_queue.front();
            has_task = true;
        }
        
        // Execute task
        bool should_notify_sync = false;
        if (has_task) {
            int cur_task_id = task->curr_task_id++;
            lock.unlock();
            task->runnable->runTask(cur_task_id, task->total_tasks);
            
            // 检查这个launch是否完成，如果完成了则更新他的后继的lanuch的depent_to vector
            lock.lock();
            task->num_completed_tasks++;
            if (task->num_completed_tasks == task->total_tasks) {
                ready_queue.erase(task_iter);
                completed_launch_ids.insert(task->launch_id);
                for (TaskID successor : task->successors) {
                    launch_id_map[successor].num_depends--;
                    if (launch_id_map[successor].num_depends == 0) {
                        ready_queue.push_back(&launch_id_map[successor]);
                    }
                }
            }

            lock.unlock();
            should_notify_sync = (ready_queue.empty());
            // should_notify_sync = (completed_launch_ids.size() == launch_id_map.size());
            
            if (should_notify_sync) {
                completed_cv.notify_all();
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // DONE: CS149 student implementations may decide to perform setup
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
    // DONE: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
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
    
    std::unique_lock<std::mutex> lock(queue_mutex);
    
     // 为每个任务创建TaskLaunch对象
    // 在 map 中创建/放置该 launch 的对象，获取其稳定地址
    std::pair<std::unordered_map<TaskID, TaskLaunch>::iterator, bool> emplace_result =
        launch_id_map.emplace(launch_id, TaskLaunch(launch_id, runnable, 0, num_total_tasks, 0, 0, num_total_tasks, {}));
    TaskLaunch* task_ptr = &emplace_result.first->second;
    

    // 检查是否已经已有依赖完成了，如果有把deps里面完成了的依赖过滤掉
    for (TaskID dep_id : deps) {
        if (completed_launch_ids.find(dep_id) == completed_launch_ids.end()) {
            
            task_ptr->num_depends++;
            // 记录依赖 -> 后继 关系
            launch_id_map[dep_id].successors.push_back(launch_id);
        }
    }
    if (task_ptr->num_depends == 0) {
        ready_queue.push_back(task_ptr);
    }


    lock.unlock();
    
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
    completed_cv.wait(lock, [this] { return ready_queue.empty(); });
}