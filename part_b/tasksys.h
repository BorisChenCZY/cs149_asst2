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
#include <unordered_map>

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

struct Job {
    IRunnable* runnable;
    int total_tasks;
    int job_id;
    std::atomic<int> remaining_deps{0};
    std::atomic<int> tasks_remaining{0};
};

struct Task {
    IRunnable* runnable;
    int task_id;
    int total_tasks;
    TaskID job_id;
};

class DependencyGraph;  // Forward declaration

class Runtime {
public:
    void add_jobs(const std::vector<Job*>& jobs) {
        std::lock_guard<std::mutex> lock(m_queue_mutex);

        // Reset counters - this is safe because sync() waits for completion before calling this
        m_complete_tasks.store(0);
        m_total_tasks = 0;

        // Add all tasks from all jobs to the queue
        for (const auto& job : jobs) {
            for (int i = 0; i < job->total_tasks; i++) {
                Task task;
                task.runnable = job->runnable;
                task.task_id = i;
                task.total_tasks = job->total_tasks;
                task.job_id = job->job_id;
                m_tasks.push(task);
                m_total_tasks++;
            }
        }
        m_task_cv.notify_all();
    }

    void add_job(IRunnable* runnable, int total_tasks) {
        std::lock_guard<std::mutex> lock(m_queue_mutex);

        // Reset counters - this is safe because run() waits for completion before calling this
        m_complete_tasks.store(0);
        m_total_tasks = 0;

        for (int i = 0; i < total_tasks; i++) {
            Task task;
            task.runnable = runnable;
            task.task_id = i;
            task.total_tasks = total_tasks;
            task.job_id = -1;
            m_tasks.push(task);
            m_total_tasks++;
        }
        m_task_cv.notify_all();
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(m_queue_mutex);
        return m_tasks.empty();
    }

    void add_complete() {
        m_complete_tasks++;
    }

    bool is_all_complete() {
        return m_complete_tasks.load() >= m_total_tasks;
    }

    void set_dep_graph(DependencyGraph* dep_graph) {
        m_dep_graph = dep_graph;
    }

    void init_job_tracking(size_t num_jobs) {
        m_jobs_completed.store(0);
        m_total_jobs = num_jobs;
    }

    bool all_jobs_complete() {
        return m_jobs_completed.load() >= m_total_jobs;
    }

    std::mutex m_queue_mutex;
    std::condition_variable m_task_cv;
    std::queue<Task> m_tasks;
    int m_total_tasks;
    std::atomic<int> m_complete_tasks{0};

    // Dependency-driven execution tracking
    DependencyGraph* m_dep_graph = nullptr;
    std::atomic<size_t> m_jobs_completed{0};
    size_t m_total_jobs = 0;
};

class DependencyGraph {
    public:
        ~DependencyGraph() {
            for (Job* job : m_jobs) {
                delete job;
            }
        }

        TaskID add_job(IRunnable* runnable, int total_tasks, const std::vector<TaskID>& deps) {
            TaskID new_id = m_jobs.size();

            for (TaskID dep : deps) {
                if (dep >= new_id) {
                    assert(false && "Dependency on non-existent job");
                    return -1;
                }
            }

            Job* job = new Job();
            job->runnable = runnable;
            job->total_tasks = total_tasks;
            job->job_id = new_id;
            job->remaining_deps.store(deps.size());
            job->tasks_remaining.store(total_tasks);
            m_jobs.push_back(job);

            m_topo_order.push_back(new_id);

            // Build reverse adjacency list: for each dependency, track that this job depends on it
            m_dependents.resize(new_id + 1);
            for (TaskID dep : deps) {
                m_dependents[dep].push_back(new_id);
            }

            return new_id;
        }

        const std::vector<TaskID>& get_topo_order() const {
            return m_topo_order;
        }

        std::vector<Job*> get_topology_sort() {
            std::vector<Job*> sorted_jobs;
            sorted_jobs.reserve(m_topo_order.size());

            for (TaskID id : m_topo_order) {
                sorted_jobs.push_back(m_jobs[id]);
            }

            return sorted_jobs;
        }

        Job* get_job(TaskID id) {
            if (id < m_jobs.size()) {
                return m_jobs[id];
            }
            return nullptr;
        }

        bool deps_completed(TaskID id) {
            // This method is unused but kept for API compatibility
            // In the optimized version, we track dependencies via remaining_deps atomic
            if (id >= m_jobs.size()) return false;
            return m_jobs[id]->remaining_deps.load() == 0;
        }

        size_t size() const {
            return m_jobs.size();
        }

        const std::vector<TaskID>& get_dependents(TaskID id) const {
            static const std::vector<TaskID> empty;
            if (id >= m_dependents.size()) return empty;
            return m_dependents[id];
        }

        mutable std::mutex m_graph_mutex;
        std::condition_variable m_graph_cv;
        std::vector<Job*> m_jobs;
        std::vector<TaskID> m_topo_order;  // Pre-computed during insertion
        std::vector<std::vector<TaskID>> m_dependents;  // m_dependents[i] = jobs that depend on job i
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
