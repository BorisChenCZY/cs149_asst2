#ifndef _TASKSYS_NEW_H
#define _TASKSYS_NEW_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

/*
 * A lean Part B implementation inspired by Lfalive's approach but adapted
 * to our Part A thread-pool style. It uses:
 *  - Per-launch remaining dependency count (remainingDeps)
 *  - Reverse edges (dependents) to unlock children immediately when a parent finishes
 *  - A single global ready task queue consumed by sleeping worker threads
 */

struct TSN_TaskGroup {
    TaskID id;
    IRunnable* runnable;
    int numTotalTasks;
    std::atomic<int> tasksRemaining{0};
    std::atomic<int> remainingDeps{0};
    std::vector<TaskID> dependents; // children launches

    TSN_TaskGroup(TaskID id_, IRunnable* r, int n, int deps)
        : id(id_), runnable(r), numTotalTasks(n) {
        tasksRemaining.store(n);
        remainingDeps.store(deps);
    }
};

struct TSN_Task {
    std::shared_ptr<TSN_TaskGroup> group;
    int taskId;
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();

private:
    // worker pool
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};

    // global scheduling state
    std::mutex mtx_;
    std::condition_variable cvTasks_;
    std::queue<TSN_Task> readyTasks_;

    std::unordered_map<TaskID, std::shared_ptr<TSN_TaskGroup>> groups_;
    std::unordered_map<TaskID, std::vector<TaskID>> pendingDependents_; // when parent not yet created

    std::atomic<TaskID> nextId_{1};
    std::atomic<int> pendingLaunches_{0};
    std::condition_variable cvAllDone_;
    std::mutex mtxAllDone_;

    void workerLoop();
    void enqueueGroupTasksLocked(const std::shared_ptr<TSN_TaskGroup>& g);
};

#endif


