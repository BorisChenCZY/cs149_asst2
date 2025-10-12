#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>
#include <condition_variable>

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

class Runtime {
public:
    void add_job(IRunnable* runnable, int total_tasks) {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        m_complete_tasks = 0;
        for (int i = 0; i < total_tasks; i++) {
            m_jobs.push(i);
        }
        m_total_tasks = total_tasks;
        m_runnable = runnable;
    }

    void run(int i)
    {
        m_runnable->runTask(i, m_total_tasks);
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        return m_jobs.empty();
    }

    int pop() {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        if (m_jobs.empty()) return -1;
        int job = m_jobs.front();
        m_jobs.pop();
        return job;
    }

    void add_complete() {
        m_complete_tasks++;
    }

    bool is_all_complete() {
        return m_complete_tasks.load() >= m_total_tasks;
    }

    std::mutex m_queue_mutex;
    std::queue<int> m_jobs;
    int m_total_tasks;
    std::atomic<int> m_complete_tasks{0};
    IRunnable* m_runnable;
};

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

        Runtime m_runtime;
        std::vector<std::thread> m_threads;
        std::atomic<bool> m_done{false};
        std::mutex m_completed_mutex;
        std::condition_variable m_completed_cv;
};

#endif
