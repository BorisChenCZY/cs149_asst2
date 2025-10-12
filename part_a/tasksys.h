#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>

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
    void add_job(IRunnable* runnable, int total_tasks, int num_threads) {
        m_total_tasks = total_tasks;
        m_next_tasks.store(0);
        m_complete_tasks.store(0);
        m_runnable = runnable;
        m_thread_wait.store(0);
        m_thread_started.store(0);
        m_num_threads = num_threads;
        m_active.store(true);
    }

    void reset() {
        m_active.store(false);
    }

    void run(int i)
    {
        m_runnable->runTask(i, m_total_tasks);
    }

    int next_task() {
        return m_next_tasks.fetch_add(1);
    }

    bool started() {
        return m_active.load();
    }
    
    bool completed() {
        int complete = m_complete_tasks.load();
        return complete != 0 && complete >= m_total_tasks;
    }

    void mark_complete() {
        m_complete_tasks.fetch_add(1);
    }

    void wait_thread() {
        m_thread_wait.fetch_add(1);
    }

    bool is_all_thread_ready() {
        return m_thread_wait.load() >= m_num_threads;
    }

    void start_thread() {
        m_thread_started.fetch_add(1);
    } 

    bool is_all_thread_started() {
        return m_thread_started.load() >= m_num_threads;
    }   

    std::atomic<int> m_next_tasks{0};
    std::atomic<int> m_complete_tasks{0};
    std::atomic<bool> m_active{false};
    std::atomic<int> m_thread_wait{0};
    std::atomic<int> m_thread_started{0};
    int m_total_tasks;
    int m_num_threads;
    IRunnable* m_runnable;
};

template <typename Context, bool spinning = false>
inline void thread_executor(int thread_id, Context* context) {
    auto &m_done = context->m_done;
    auto& runtime = context->m_runtime;

    auto yield = [](){
        if (not spinning) {
            sched_yield();
        }
    };

    while (true) {
        while (not m_done && not runtime.started()) yield();
        if (m_done) {
            debug_print("Thread %d exiting\n", thread_id);
            break;
        }

        runtime.start_thread();
        while (not runtime.is_all_thread_started()) yield();

        debug_print("Thread %d started, total_tasks: %d\n", thread_id, runtime.m_total_tasks);
        while (not runtime.completed())
        {
            int process_id = runtime.next_task();
            if (process_id >= runtime.m_total_tasks) {
                debug_print("Thread %d no more tasks, exiting\n", thread_id);
                break;
            }
            runtime.run(process_id);
            runtime.mark_complete();
        }

        debug_print("Thread %d finished, is_completed: %b, tasks: %d\n", thread_id, runtime.completed(), runtime.m_complete_tasks.load());
        while (not runtime.completed()) yield();
        runtime.reset();
        runtime.wait_thread();
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
};

#endif
