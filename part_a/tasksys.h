#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
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
        m_runnable = runnable;
        m_next_tasks = 0;

        m_complete_tasks= 0;
        m_total_tasks = total_tasks;

        m_active.store(true, std::memory_order::memory_order_release);
    }

    void run(int i)
    {
        m_runnable->runTask(i, m_total_tasks);
    }

    int next_task() {
        return m_next_tasks++;
    }
    
    bool completed() {
        // std::unique_lock<std::mutex> lock(m_complete_mutex);
        return completed_no_lock();
    }

    void mark_complete() {
        // std::unique_lock<std::mutex> lock(m_complete_mutex);
        m_complete_tasks.fetch_add(1, std::memory_order::memory_order_release);
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
        m_complete_cv.wait(lock, [this]{ return completed_no_lock(); });
    }

    bool completed_no_lock() const {
        return m_complete_tasks >= m_total_tasks;
    }

public:

    volatile int m_total_tasks{0};
    IRunnable* m_runnable;
    std::mutex m_complete_mutex;
    std::condition_variable m_complete_cv;
    std::condition_variable m_next_cv;
    alignas(64) std::atomic<int> m_next_tasks{0} ;
    alignas(64) std::atomic<int> m_complete_tasks{0};
    alignas(64) std::atomic<bool> m_active{false};
};

template <typename Context, bool spinning = false>
inline void thread_executor(int thread_id, Context* context) {
    auto &m_done = context->m_done;
    auto &runtime = context->m_runtime;

    while (not m_done) {
        while (runtime.m_active and not runtime.completed())
        {
            int process_id = runtime.next_task();
            // debug_print("Thread %d picked task %d\n", thread_id, process_id);
            if (process_id >= runtime.m_total_tasks) {
                debug_print("Thread %d no more tasks, exiting\n", thread_id);
                break;
            }
            runtime.run(process_id);
            runtime.mark_complete();
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
                    return m_done || (runtime.m_active.load() and not runtime.completed_no_lock());
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
