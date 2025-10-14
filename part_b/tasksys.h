#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <cassert>

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

struct Job {
    IRunnable* runnable;
    int total_tasks;
    std::vector<TaskID> deps;
    bool completed = false;
    int job_id;
};

class DependencyGraph {
    public:
        TaskID add_job(IRunnable* runnable, int total_tasks, const std::vector<TaskID>& deps) {
            TaskID new_id = m_jobs.size();

            for (TaskID dep : deps) {
                if (dep >= new_id) {
                    assert(false && "Dependency on non-existent job");
                    return -1;
                }
            }

            Job job;
            job.runnable = runnable;
            job.total_tasks = total_tasks;
            job.deps = deps;
            job.completed = false;
            job.job_id = new_id;
            m_jobs.push_back(job);

            m_topo_order.push_back(new_id);

            return new_id;
        }

        const std::vector<TaskID>& get_topo_order() const {
            return m_topo_order;
        }

        std::vector<Job> get_topology_sort() {
            std::vector<Job> sorted_jobs;
            sorted_jobs.reserve(m_topo_order.size());

            for (TaskID id : m_topo_order) {
                sorted_jobs.push_back(m_jobs[id]);
            }

            return sorted_jobs;
        }

        Job* get_job(TaskID id) {
            if (id < m_jobs.size()) {
                return &m_jobs[id];
            }
            return nullptr;
        }

        void mark_completed(TaskID id) {
            if (id < m_jobs.size()) {
                m_jobs[id].completed = true;
            }
        }

        bool deps_completed(TaskID id) {
            if (id >= m_jobs.size()) return false;

            for (TaskID dep : m_jobs[id].deps) {
                if (dep >= m_jobs.size() || !m_jobs[dep].completed) {
                    return false;
                }
            }
            return true;
        }

        size_t size() const {
            return m_jobs.size();
        }

        std::vector<Job> m_jobs;
        std::vector<TaskID> m_topo_order;  // Pre-computed during insertion
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

    public:
        Runtime m_runtime;
        DependencyGraph m_dep_graph;
        std::vector<std::thread> m_threads;
        std::atomic<bool> m_done{false};
        std::mutex m_completed_mutex;
        std::condition_variable m_completed_cv;
        int m_max_threads = 0;
};

#endif
