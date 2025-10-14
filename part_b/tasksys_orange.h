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

// TaskBulk structure for Part B - Orange's implementation
struct TaskBulk {
    TaskID id;
    IRunnable* runnable;
    int num_total_tasks = 0;
    int num_tasks_done = 0;
    int next_task_id = 0;
    std::unordered_set<TaskID> dependencies;
    std::mutex bulk_mtx;
    
    TaskBulk(TaskID _id, IRunnable* _runnable, int _num_total_tasks, const std::vector<TaskID>& _deps) 
        : id(_id), runnable(_runnable), num_total_tasks(_num_total_tasks), 
          dependencies(_deps.begin(), _deps.end()) {}
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
        int num_threads;
        TaskID next_id;                        // 主线程读写，子线程会多次读(确认所有任务完成时)，不需要加锁
        bool done;                             // 所有任务完成，主/子线程据此休眠
        bool terminate;                       
        std::atomic<int> num_bulk_done;        // 多个线程更新
        std::thread* thread_pool;              
        std::vector<TaskBulk*> waiting;        
        std::vector<TaskBulk*> ready;          
        std::unordered_set<TaskBulk*> all_bulks;  // 用于析构
        std::unordered_set<TaskID> finished_bulk; // 用于依赖关系判断
        std::condition_variable sleeping_cv;
        std::condition_variable waiting_cv;
        std::mutex qmtx;                       // 队列的锁
        std::mutex cmtx;                       // 读写done的锁，用于两个条件变量
        
        void ThreadFunc();
        void CheckWaiting();
};

#endif
