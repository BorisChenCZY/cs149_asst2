#include "tasksys_new.h"
#include <cassert>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

const char* TaskSystemParallelThreadPoolSleeping::name() { return "Parallel + Thread Pool + Sleep"; }

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    workers_.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    stop_.store(true);
    cvTasks_.notify_all();
    for (auto& t : workers_) t.join();
}

void TaskSystemParallelThreadPoolSleeping::enqueueGroupTasksLocked(const std::shared_ptr<TSN_TaskGroup>& g) {
    bool wasEmpty = readyTasks_.empty();
    for (int i = 0; i < g->numTotalTasks; ++i) {
        readyTasks_.push(TSN_Task{g, i});
    }
    if (wasEmpty) cvTasks_.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (!stop_.load()) {
        TSN_Task t{};
        bool has = false;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cvTasks_.wait(lk, [&]{ return stop_.load() || !readyTasks_.empty(); });
            if (stop_.load()) break;
            if (!readyTasks_.empty()) {
                t = readyTasks_.front();
                readyTasks_.pop();
                has = true;
            }
        }
        if (!has) continue;

        // run task
        t.group->runnable->runTask(t.taskId, t.group->numTotalTasks);

        // completion bookkeeping
        bool notifyAllDone = false;
        std::vector<std::shared_ptr<TSN_TaskGroup>> toEnqueue;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            int left = t.group->tasksRemaining.fetch_sub(1) - 1;
            if (left == 0) {
                // this group finished: unlock dependents
                for (TaskID childId : t.group->dependents) {
                    auto itc = groups_.find(childId);
                    if (itc != groups_.end()) {
                        int r = itc->second->remainingDeps.fetch_sub(1) - 1;
                        if (r == 0) {
                            toEnqueue.push_back(itc->second);
                        }
                    }
                }
                // one launch done
                int pend = pendingLaunches_.fetch_sub(1) - 1;
                if (pend == 0) notifyAllDone = true;
            }
        }
        if (!toEnqueue.empty()) {
            std::lock_guard<std::mutex> lk(mtx_);
            for (auto& g : toEnqueue) enqueueGroupTasksLocked(g);
        }
        if (notifyAllDone) {
            std::lock_guard<std::mutex> lk(mtxAllDone_);
            cvAllDone_.notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> none;
    runAsyncWithDeps(runnable, num_total_tasks, none);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps) {
    TaskID id = nextId_.fetch_add(1);
    auto g = std::make_shared<TSN_TaskGroup>(id, runnable, num_total_tasks, (int)deps.size());

    {
        std::lock_guard<std::mutex> lk(mtx_);
        groups_[id] = g;
        pendingLaunches_.fetch_add(1);

        // register dependents
        for (TaskID d : deps) {
            auto it = groups_.find(d);
            if (it != groups_.end()) it->second->dependents.push_back(id);
            else pendingDependents_[d].push_back(id);
        }

        // migrate pending dependents for this newly created group (as parent)
        auto pend = pendingDependents_.find(id);
        if (pend != pendingDependents_.end()) {
            // id is a parent; attach its pending children
            // Note: this migration is useful only if some child arrived before parent
            // We attach to this group's dependents so when this group completes, children unlock
            for (TaskID child : pend->second) g->dependents.push_back(child);
            pendingDependents_.erase(pend);
        }

        if (g->remainingDeps.load() == 0) {
            enqueueGroupTasksLocked(g);
        }
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(mtxAllDone_);
    cvAllDone_.wait(lk, [&]{ return pendingLaunches_.load() == 0; });
}


