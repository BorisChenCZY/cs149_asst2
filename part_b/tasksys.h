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
#include <deque>
#include <algorithm>

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

struct TaskLaunch {
    TaskID launch_id;
    IRunnable* runnable;
    int curr_task_id;
    int total_tasks;
    int num_completed_tasks;
    int num_depends; // 要依赖多少个之前的launch
    int num_task_remaining; // 要执行多少个task
    std::vector<TaskID> successors; // 后继
    // std::vector<TaskID> depend_on;  // 依赖谁
    
    // Default constructor
    TaskLaunch() : launch_id(0), runnable(nullptr), curr_task_id(0), total_tasks(0), num_completed_tasks(0), num_depends(0), num_task_remaining(0), successors(std::vector<TaskID>()) {}
    
    // Parameterized constructor
    TaskLaunch(TaskID launch_id, IRunnable* runnable, int curr_task_id, int total_tasks, int num_completed_tasks, int num_depends, int num_task_remaining, std::vector<TaskID> successors) 
        : launch_id(launch_id), runnable(runnable), curr_task_id(curr_task_id), total_tasks(total_tasks), num_completed_tasks(num_completed_tasks), num_depends(num_depends), num_task_remaining(num_task_remaining), successors(successors) {}

    bool operator<(const TaskLaunch& other) const {
        return successors.size() > other.successors.size();
    }
};


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
        // 线程管理
        int num_threads;
        std::vector<std::thread> threads;
        std::atomic<bool> terminate{false};
        std::atomic<TaskID> next_launch_id{0};  // 从0开始，0表示无效，1表示第一个Launch，一个launch有多个task
        
        // 全局依赖管理
        std::unordered_map<TaskID, TaskLaunch> launch_id_map;        // 现在变成launchID对TaskLaunch的对应map了 --- 依赖关系DAG: dep_id -> [dependent_launches_ids]
        std::deque<TaskLaunch*> ready_queue;                     // 可执行任务队列
        // std::queue<TaskLaunch*> ready_queue;  

        // 任务状态跟踪
        std::unordered_set<TaskID> completed_launch_ids;             // 已完成任务
        
        // 同步机制
        std::mutex queue_mutex;                                 // 保护队列和依赖关系
        std::mutex completed_mutex;                             // 保护已完成任务
        std::condition_variable queue_cv;       
        std::condition_variable completed_cv;                   // 任务可用时通知
        // std::atomic<int> pending_tasks{0};                   // 待完成任务数
        
        // Helper methods
        void worker_thread_function();                          // 工作线程函数
};

#endif
