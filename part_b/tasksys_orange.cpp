#include "tasksys_orange.h"
#include <algorithm>
#include <cstdlib>

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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_bulk_done(0) {
    this->num_threads = num_threads;
    terminate = false;
    done = true;
    next_id = 0;
    thread_pool = new std::thread[num_threads];
    for(int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::ThreadFunc, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    sync();
    terminate = true;
    // 唤醒所有线程并终止
    std::unique_lock<std::mutex> lk(cmtx);
    done = false;
    sleeping_cv.notify_all();
    lk.unlock();
    for(int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }
    for(auto bulk: all_bulks) {
        delete bulk;
    }
    delete[] thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> noDeps;
    runAsyncWithDeps(runnable, num_total_tasks, noDeps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    TaskID the_id = next_id++;
    TaskBulk* new_bulk = new TaskBulk(the_id, runnable, num_total_tasks, std::move(deps));
    all_bulks.insert(new_bulk);
    
    // enqueue task_bulk
    qmtx.lock();
    if(deps.size() == 0) {
        ready.push_back(new_bulk);
    } else {
        waiting.push_back(new_bulk);
    }
    qmtx.unlock();
    
    cmtx.lock();
    done = false;
    cmtx.unlock();
    sleeping_cv.notify_all();
    return the_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(cmtx);
    while(!done) {
        waiting_cv.wait(lk);
        if(num_bulk_done == next_id) {
            done = true;
        } else {
            done = false;
            sleeping_cv.notify_all();
        }
    }
    lk.unlock();
}

void TaskSystemParallelThreadPoolSleeping::CheckWaiting() {
    std::vector<TaskBulk*> move;
    for(auto waiting_bulk: waiting) {
        for (auto it = waiting_bulk->dependencies.begin(); it != waiting_bulk->dependencies.end(); ) {
            if (finished_bulk.find(*it) != finished_bulk.end()) {
                it = waiting_bulk->dependencies.erase(it);
            } 
            else {
                ++it;
            }
        }
        if(waiting_bulk->dependencies.size() == 0) {
            move.push_back(waiting_bulk);
        }
    }
    for(auto remov: move) {
        auto it = std::find(waiting.begin(), waiting.end(), remov);
        waiting.erase(it);
    }
    for(auto enqueue: move) {
        ready.push_back(enqueue); 
    }
}

void TaskSystemParallelThreadPoolSleeping::ThreadFunc() {
    int my_id, num_done;
    TaskBulk* my_bulk;
    while(!terminate) {
        // 检查是否所有bulk都已经完成
        std::unique_lock<std::mutex> lk(cmtx);
        if(terminate) return;    // 此时可能主线程已经在析构中join了，此时线程获取cmtx，要检查terminate
        if(next_id == num_bulk_done) {
            done = true;
            waiting_cv.notify_one();
        } else {
            done = false;
        }
        while(done) {
            waiting_cv.notify_one();
            sleeping_cv.wait(lk);
            if(terminate) return; // 被唤醒的原因：被终止或有新任务
            if(next_id == num_bulk_done) {
                done = true;
                waiting_cv.notify_one();
            } else {
                done = false;
            }
        }
        lk.unlock();
        
        // 尝试获取task
        my_id = 0x3f3f3f3f;
        my_bulk = nullptr;
        qmtx.lock();
        if(ready.empty()) {
            CheckWaiting(); // 检查是否有就绪任务组
        }
        if(!ready.empty()) {
            my_bulk = ready[rand() % ready.size()]; // 通过随机选择减少等在同一个bulk
            my_bulk->bulk_mtx.lock();
            my_id = my_bulk->next_task_id++;
            my_bulk->bulk_mtx.unlock();
        }
        qmtx.unlock();
        
        // 执行task
        if(my_bulk && my_id < my_bulk->num_total_tasks) {
            my_bulk->runnable->runTask(my_id, my_bulk->num_total_tasks);
            my_bulk->bulk_mtx.lock();
            num_done = ++my_bulk->num_tasks_done;
            my_bulk->bulk_mtx.unlock();
            if(num_done == my_bulk->num_total_tasks) {
                // 从ready队列删除bulk
                qmtx.lock();
                num_bulk_done++;
                auto it = std::find_if(ready.begin(), ready.end(), 
                    [my_bulk](const TaskBulk* ptr) {return ptr == my_bulk;});
                if(it != ready.end()) {
                    ready.erase(it);
                }
                finished_bulk.insert(my_bulk->id);
                sleeping_cv.notify_all();
                qmtx.unlock();
            }
        }
    }
}
