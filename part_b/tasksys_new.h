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
 * TaskSystemParallelThreadPoolSleeping: Optimized implementation inspired by Lfalive's approach.
 * Uses remaining dependency count + reverse edges for efficient task scheduling.
 */

struct TaskGroup {
    TaskID id;
    IRunnable* runnable;
    int numTotalTasks;
    std::atomic<int> tasksRemaining{0};
    std::atomic<int> remainingDeps{0};
    std::vector<TaskID> dependents; // children launches

    TaskGroup(TaskID id_, IRunnable* r, int n, int deps)
        : id(id_), runnable(r), numTotalTasks(n) {
        tasksRemaining.store(n);
        remainingDeps.store(deps);
    }
};

struct RunnableTask {
    std::shared_ptr<TaskGroup> group;
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
    std::vector<std::thread> workers;
    std::atomic<bool> stop{false};

    // global scheduling state
    std::mutex queueMutex;
    std::condition_variable queueCond;
    std::queue<RunnableTask> readyTasks;

    std::unordered_map<TaskID, std::shared_ptr<TaskGroup>> groups;
    std::unordered_map<TaskID, std::vector<TaskID>> pendingDependents;

    std::atomic<TaskID> nextId{1};
    std::atomic<int> pendingLaunches{0};
    std::condition_variable allDoneCond;
    std::mutex allDoneMutex;

    void workerLoop();
    void enqueueGroupTasksLocked(const std::shared_ptr<TaskGroup>& g);
};

#endif


