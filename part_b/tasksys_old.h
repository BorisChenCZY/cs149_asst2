#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
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

// Task structure for Part B
struct Task {
    IRunnable* runnable;
    int task_id;
    int num_total_tasks;
    TaskID launch_id;  // Which bulk launch this task belongs to
};

// Task launch information
struct TaskLaunch {
    TaskID id;
    IRunnable* runnable;
    int num_total_tasks;
    std::vector<TaskID> dependencies;
    std::atomic<int> remainingDeps{0};
    std::vector<TaskID> dependents;
    std::atomic<int> completed_tasks{0};
    std::atomic<bool> is_complete{false};
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

    private:
        // Part A members (from your teammate's implementation)
        Runtime m_runtime;
        std::vector<std::thread> m_threads;
        std::atomic<bool> m_done{false};
        std::mutex m_completed_mutex;
        std::condition_variable m_completed_cv;
        int m_max_threads = 0;

        // Part B members for async task execution and dependencies
        std::atomic<TaskID> m_next_task_id{1};  // Start from 1, 0 reserved for invalid
        
        // Simplified task management
        std::mutex m_task_mutex;
        std::queue<Task> m_ready_queue;  // Tasks ready to execute
        std::condition_variable m_task_cv;  // Signal when tasks are available
        
        // Simplified dependency tracking
        std::unordered_map<TaskID, std::shared_ptr<TaskLaunch>> m_task_launches;
        std::unordered_map<TaskID, std::unordered_set<TaskID>> m_dependents;  // Which launches depend on this one (legacy)
        
        // Sync tracking
        std::atomic<int> m_pending_launches{0};  // Number of launches not yet complete
        
        // Helper methods
        bool are_dependencies_satisfied(const std::vector<TaskID>& deps);
        void move_ready_tasks_to_queue(TaskID completed_launch_id);
        void worker_thread_function();
};

#endif
