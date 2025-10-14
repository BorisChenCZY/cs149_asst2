#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <vector>

template <typename ...Args>
inline void debug_print(const char* msg, Args... args) {
    #ifdef DEBUG
    static std::mutex print_mutex;
    std::lock_guard<std::mutex> lock(print_mutex);
    printf(msg, args...);
    #endif
}

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

struct Task {
    IRunnable* runnable;
    int total_tasks;
    std::atomic<int> current_processed_id;
    std::atomic<int> finished_count;
    bool is_ready;

    Task(IRunnable* r, int total, bool ready = true)
        : runnable(r), total_tasks(total), current_processed_id(0),
          finished_count(0), is_ready(ready) {}

    // Move constructor
    Task(Task&& other)
        : runnable(other.runnable), total_tasks(other.total_tasks),
          current_processed_id(other.current_processed_id.load()),
          finished_count(other.finished_count.load()),
          is_ready(other.is_ready) {}

    // Delete copy constructor since atomics aren't copyable
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task& operator=(Task&&) = delete;

    bool is_done() const {
        return finished_count.load(std::memory_order_acquire) >= total_tasks;
    }

    int get_next_task_id() {
        return current_processed_id.fetch_add(1, std::memory_order_acq_rel);
    }

    void mark_task_complete() {
        finished_count.fetch_add(1, std::memory_order_release);
    }
};

class Runtime {
public:
    void add_job(IRunnable* runnable, int total_tasks) {
        std::unique_lock<std::mutex> lock(m_task_mutex);
        m_tasks.push_back(std::make_shared<Task>(runnable, total_tasks, true));
        m_active.store(true, std::memory_order_release);
    }

    std::shared_ptr<Task> get_next_ready_task() {
        std::unique_lock<std::mutex> lock(m_task_mutex);

        // Find first ready but not done task, and remove completed ones
        for (auto it = m_tasks.begin(); it != m_tasks.end(); ) {
            auto& task = *it;
            if (task->is_done()) {
                // Remove completed task from queue
                it = m_tasks.erase(it);
            } else if (task->is_ready) {
                // Found a ready task, return shared_ptr to it
                return task;
            } else {
                ++it;
            }
        }
        // No ready tasks available
        return nullptr;
    }

    bool all_tasks_done() {
        std::unique_lock<std::mutex> lock(m_task_mutex);
        for (const auto& task : m_tasks) {
            if (!task->is_done()) {
                return false;
            }
        }
        return true;
    }

    bool completed() {
        return all_tasks_done();
    }

    void mark_complete(std::shared_ptr<Task> task) {
        task->mark_task_complete();

        // Only notify when ALL tasks are done
        if (all_tasks_done()) {
            notify_complete();
        }
    }

    void clear_tasks() {
        std::unique_lock<std::mutex> lock(m_task_mutex);
        m_tasks.clear();
    }

    void notify_complete() {
        m_complete_cv.notify_one();
    }

    void notify_next() {
        m_next_cv.notify_one();
    }

    void notify_next_all() {
        m_next_cv.notify_all();
    }

    void wait_complete() {
        std::unique_lock<std::mutex> lock(m_complete_mutex);
        m_complete_cv.wait(lock, [this]{ return all_tasks_done(); });
    }

public:
    std::vector<std::shared_ptr<Task>> m_tasks;
    std::mutex m_task_mutex;
    std::mutex m_complete_mutex;
    std::condition_variable m_complete_cv;
    std::condition_variable m_next_cv;
    alignas(64) std::atomic<bool> m_active{false};
};

template <typename Context, bool spinning = false>
inline void thread_executor(int thread_id, Context* context) {
    auto &m_done = context->m_done;
    auto &runtime = context->m_runtime;

    while (not m_done) {
        // Get shared work object (outside the loop)
        std::shared_ptr<Task> work = runtime.get_next_ready_task();

        // Loop until this work is exhausted
        while (work != nullptr && runtime.m_active && !runtime.completed())
        {
            // Atomically get next task ID from shared work object
            int task_id = work->get_next_task_id();

            // Check if this task is exhausted
            if (task_id >= work->total_tasks) {
                // This task is done, get new work
                debug_print("Thread %d exhausted task, getting new work\n", thread_id);
                work = runtime.get_next_ready_task();
                continue;
            }

            // Execute the task
            debug_print("Thread %d picked task %d from shared work\n", thread_id, task_id);
            work->runnable->runTask(task_id, work->total_tasks);

            // Mark complete (will notify if all tasks done)
            runtime.mark_complete(work);
        }

        if (work == nullptr) {
            // No ready tasks available
            debug_print("Thread %d no ready tasks available\n", thread_id);
        }

        if (not spinning and runtime.completed())
        {
            context->notify();
        }

        if (not spinning) {
            /*
            {
                std::unique_lock<std::mutex> lk(runtime.m_complete_mutex);
                runtime.m_next_cv.wait(lk, [&](){
                    return m_done || (runtime.m_active.load() and not runtime.all_tasks_done());
                });
            }
            runtime.notify_next();
            */
            std::this_thread::yield();
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void notify() { }

    public:
        Runtime m_runtime;
        std::vector<std::thread> m_threads;
        std::atomic<bool> m_done{false};
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void notify() { m_runtime.notify_complete(); }

        Runtime m_runtime;
        std::vector<std::thread> m_threads;
        std::atomic<bool> m_done{false};
};

#endif
