#include "tasksys_new.h"
#include <cassert>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

const char* TaskSystemParallelThreadPoolSleeping::name() { return "Parallel + Thread Pool + Sleep"; }

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    stop.store(true);
    queueCond.notify_all();
    for (auto& t : workers) t.join();
}

void TaskSystemParallelThreadPoolSleeping::enqueueGroupTasksLocked(const std::shared_ptr<TaskGroup>& g) {
    bool wasEmpty = readyTasks.empty();
    for (int i = 0; i < g->numTotalTasks; ++i) {
        readyTasks.push(RunnableTask{g, i});
    }
    if (wasEmpty) queueCond.notify_all();
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (!stop.load()) {
        RunnableTask t{};
        bool has = false;
        {
            std::unique_lock<std::mutex> lk(queueMutex);
            queueCond.wait(lk, [&]{ return stop.load() || !readyTasks.empty(); });
            if (stop.load()) break;
            if (!readyTasks.empty()) {
                t = readyTasks.front();
                readyTasks.pop();
                has = true;
            }
        }
        if (!has) continue;

        // run task
        t.group->runnable->runTask(t.taskId, t.group->numTotalTasks);

        // completion bookkeeping
        bool notifyAllDone = false;
        std::vector<std::shared_ptr<TaskGroup>> toEnqueue;
        {
            std::lock_guard<std::mutex> lk(queueMutex);
            int left = t.group->tasksRemaining.fetch_sub(1) - 1;
            if (left == 0) {
                // this group finished: unlock dependents
                for (TaskID childId : t.group->dependents) {
                    auto itc = groups.find(childId);
                    if (itc != groups.end()) {
                        int r = itc->second->remainingDeps.fetch_sub(1) - 1;
                        if (r == 0) {
                            toEnqueue.push_back(itc->second);
                        }
                    }
                }
                // one launch done
                int pend = pendingLaunches.fetch_sub(1) - 1;
                if (pend == 0) notifyAllDone = true;
            }
        }
        if (!toEnqueue.empty()) {
            std::lock_guard<std::mutex> lk(queueMutex);
            for (auto& g : toEnqueue) enqueueGroupTasksLocked(g);
        }
        if (notifyAllDone) {
            std::lock_guard<std::mutex> lk(allDoneMutex);
            allDoneCond.notify_all();
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
    TaskID id = nextId.fetch_add(1);
    auto g = std::make_shared<TaskGroup>(id, runnable, num_total_tasks, (int)deps.size());

    {
        std::lock_guard<std::mutex> lk(queueMutex);
        groups[id] = g;
        pendingLaunches.fetch_add(1);

        // register dependents
        for (TaskID d : deps) {
            auto it = groups.find(d);
            if (it != groups.end()) it->second->dependents.push_back(id);
            else pendingDependents[d].push_back(id);
        }

        // migrate pending dependents for this newly created group (as parent)
        auto pend = pendingDependents.find(id);
        if (pend != pendingDependents.end()) {
            // id is a parent; attach its pending children
            // Note: this migration is useful only if some child arrived before parent
            // We attach to this group's dependents so when this group completes, children unlock
            for (TaskID child : pend->second) g->dependents.push_back(child);
            pendingDependents.erase(pend);
        }

        if (g->remainingDeps.load() == 0) {
            enqueueGroupTasksLocked(g);
        }
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(allDoneMutex);
    allDoneCond.wait(lk, [&]{ return pendingLaunches.load() == 0; });
}


