#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <chrono>

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

// Base class for Task and Job
struct TaskBase {
    IRunnable* runnable;
    int total_tasks;
    std::atomic<int> current_processed_id;
    std::atomic<int> finished_count;
    bool is_ready;

    TaskBase(IRunnable* r, int total, bool ready = true)
        : runnable(r), total_tasks(total), current_processed_id(0),
          finished_count(0), is_ready(ready) {}

    virtual ~TaskBase() = default;

    // Delete copy constructor since atomics aren't copyable
    TaskBase(const TaskBase&) = delete;
    TaskBase& operator=(const TaskBase&) = delete;
    TaskBase& operator=(TaskBase&&) = delete;

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

struct Task : public TaskBase {
    Task(IRunnable* r, int total, bool ready = true)
        : TaskBase(r, total, ready) {}

    // Move constructor
    Task(Task&& other)
        : TaskBase(other.runnable, other.total_tasks, other.is_ready) {
        current_processed_id.store(other.current_processed_id.load());
        finished_count.store(other.finished_count.load());
    }
};

struct Job : public TaskBase {
    std::atomic<int> pending_deps;
    std::vector<TaskID> dependents;
    TaskID job_id;

    Job(IRunnable* r, int total, int deps_count)
        : TaskBase(r, total, deps_count == 0), pending_deps(deps_count), job_id(-1) {}

    // Move constructor
    Job(Job&& other)
        : TaskBase(other.runnable, other.total_tasks, other.is_ready),
          pending_deps(other.pending_deps.load()),
          dependents(std::move(other.dependents)),
          job_id(other.job_id) {
        current_processed_id.store(other.current_processed_id.load());
        finished_count.store(other.finished_count.load());
    }

    // Delete copy constructor since atomics aren't copyable
    Job(const Job&) = delete;
    Job& operator=(const Job&) = delete;
    Job& operator=(Job&&) = delete;
};

class Runtime {
public:
    void add_job(IRunnable* runnable, int total_tasks) {
        std::unique_lock<std::mutex> lock(m_task_mutex);
        m_tasks.push_back(std::make_shared<Task>(runnable, total_tasks, true));
        m_active.store(true, std::memory_order_release);
    }

    TaskID add_job_with_deps(IRunnable* runnable, int total_tasks, const std::vector<TaskID>& deps) {
        std::shared_ptr<Job> job;
        TaskID job_id;

        // First, acquire jobs mutex to create job and update dependencies
        {
            std::unique_lock<std::mutex> lock(m_jobs_mutex);

            // Calculate TaskID as current size of jobs vector
            job_id = static_cast<TaskID>(m_jobs.size());

            // Create new job
            job = std::make_shared<Job>(runnable, total_tasks, deps.size());
            job->job_id = job_id;

            // Build reverse dependency map
            for (TaskID dep_id : deps) {
                // Validate dependency exists
                if (dep_id >= 0 && dep_id < static_cast<TaskID>(m_jobs.size())) {
                    m_jobs[dep_id]->dependents.push_back(job_id);
                }
            }

            // Add job to vector
            m_jobs.push_back(job);
        } // Release jobs mutex before acquiring task mutex

        // If no dependencies, add to ready queue
        // Now acquire task mutex separately to avoid deadlock
        if (deps.empty()) {
            std::unique_lock<std::mutex> task_lock(m_task_mutex);
            m_tasks.push_back(job);
        }

        m_active.store(true, std::memory_order_release);
        return job_id;
    }

    void on_job_complete(TaskID job_id) {
        std::vector<std::shared_ptr<Job>> ready_jobs;

        // First, acquire jobs mutex to process dependencies
        {
            std::unique_lock<std::mutex> lock(m_jobs_mutex);

            if (job_id < 0 || job_id >= static_cast<TaskID>(m_jobs.size())) {
                return;
            }

            auto& job = m_jobs[job_id];

            // Process all dependents
            for (TaskID dependent_id : job->dependents) {
                if (dependent_id >= 0 && dependent_id < static_cast<TaskID>(m_jobs.size())) {
                    auto& dependent = m_jobs[dependent_id];

                    // Atomically decrement pending dependencies
                    int prev_deps = dependent->pending_deps.fetch_sub(1, std::memory_order_acq_rel);

                    // If this was the last dependency, mark as ready
                    if (prev_deps == 1) {
                        dependent->is_ready = true;
                        ready_jobs.push_back(dependent);
                    }
                }
            }
        } // Release jobs mutex before acquiring task mutex

        // Now add ready jobs to task queue
        if (!ready_jobs.empty()) {
            std::unique_lock<std::mutex> task_lock(m_task_mutex);
            for (auto& ready_job : ready_jobs) {
                m_tasks.push_back(ready_job);
            }
        }

        // Notify worker threads if new work became available
        if (!ready_jobs.empty()) {
            notify_next();
        }
    }

    std::shared_ptr<TaskBase> get_next_ready_task() {
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

    // Version without locking (caller must hold m_task_mutex)
    bool all_tasks_done_unsafe() const {
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

    void mark_complete(std::shared_ptr<TaskBase> task) {
        task->mark_task_complete();

        // Check if this is a Job and handle dependency updates
        auto job = std::dynamic_pointer_cast<Job>(task);
        if (job && job->is_done()) {
            on_job_complete(job->job_id);
        }

        // Notify if all tasks are done OR all jobs are done
        if (all_tasks_done() || all_jobs_done()) {
            notify_complete();
        }
    }

    void clear_tasks() {
        std::unique_lock<std::mutex> lock(m_task_mutex);
        m_tasks.clear();
    }

    bool all_jobs_done() {
        std::unique_lock<std::mutex> lock(m_jobs_mutex);
        for (const auto& job : m_jobs) {
            if (!job->is_done()) {
                return false;
            }
        }
        return true;
    }

    // Version without locking (caller must hold m_jobs_mutex)
    bool all_jobs_done_unsafe() const {
        for (const auto& job : m_jobs) {
            if (!job->is_done()) {
                return false;
            }
        }
        return true;
    }

    void clear_jobs() {
        std::unique_lock<std::mutex> lock(m_jobs_mutex);
        m_jobs.clear();
    }

    void wait_all_jobs_complete() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(m_jobs_mutex);
                if (all_jobs_done_unsafe()) {
                    return;
                }
            }
            // Wait for notification, then recheck
            std::unique_lock<std::mutex> lock(m_complete_mutex);
            m_complete_cv.wait_for(lock, std::chrono::milliseconds(10));
        }
    }

    void notify_complete() {
        m_complete_cv.notify_all();
    }

    void notify_next() {
        m_next_cv.notify_one();
    }

    void notify_next_all() {
        m_next_cv.notify_all();
    }

    void wait_complete() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(m_task_mutex);
                if (all_tasks_done_unsafe()) {
                    return;
                }
            }
            // Wait for notification, then recheck
            std::unique_lock<std::mutex> lock(m_complete_mutex);
            m_complete_cv.wait_for(lock, std::chrono::milliseconds(10));
        }
    }

public:
    std::vector<std::shared_ptr<TaskBase>> m_tasks;
    std::vector<std::shared_ptr<Job>> m_jobs;
    std::mutex m_task_mutex;
    std::mutex m_jobs_mutex;
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
        std::shared_ptr<TaskBase> work = runtime.get_next_ready_task();

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
            // Check conditions separately to avoid nested locking in predicate
            bool should_wait = true;
            {
                std::unique_lock<std::mutex> task_lock(runtime.m_task_mutex);
                if (m_done || (runtime.m_active.load() && !runtime.all_tasks_done_unsafe())) {
                    should_wait = false;
                }
            }

            if (should_wait) {
                std::unique_lock<std::mutex> lk(runtime.m_complete_mutex);
                runtime.m_next_cv.wait_for(lk, std::chrono::milliseconds(10));
            }
            runtime.notify_next();
            // std::this_thread::yield();
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
