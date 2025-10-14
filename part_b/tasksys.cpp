#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    auto thread_run = [&](int thread_id) {
        while (not m_done) {
            Task task = m_runtime.pop();

            while (task.runnable != nullptr)
            {
                task.runnable->runTask(task.task_id, task.total_tasks);
                m_runtime.add_complete();
                task = m_runtime.pop();
            }

            // notify completed
            {
                if (m_runtime.is_all_complete()) {
                    m_completed_cv.notify_all();
                }
            }
        }
    };

    for (int i = 0; i < num_threads; i++) 
    {
        m_threads.emplace_back(thread_run, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    m_done = true;
    for (auto &t: m_threads)
    {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    debug_print("Main thread adding job with %d tasks\n", num_total_tasks);
    m_runtime.add_job(runnable, num_total_tasks);
    
    {
        std::unique_lock<std::mutex> lock(m_completed_mutex);
        m_completed_cv.wait(lock, [&]{ return m_runtime.is_all_complete(); });
    }

    debug_print("Main thread job finished\n");
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    m_dep_graph.add_job(runnable, num_total_tasks, deps);

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // Level-based execution: execute all jobs with satisfied dependencies,
    // wait for completion, then repeat until all jobs are done
    //

    size_t total_jobs = m_dep_graph.size();
    std::vector<bool> completed(total_jobs, false);
    size_t jobs_completed = 0;

    while (jobs_completed < total_jobs) {
        // Find all jobs with satisfied dependencies (ready to run)
        std::vector<Job> ready_jobs;

        for (size_t job_id = 0; job_id < total_jobs; job_id++) {
            if (completed[job_id]) continue;

            // Check if all dependencies are satisfied
            bool deps_ready = m_dep_graph.deps_completed(job_id);

            if (deps_ready) {
                Job* job = m_dep_graph.get_job(job_id);
                if (job != nullptr) {
                    ready_jobs.push_back(*job);
                    debug_print("Main thread: Job %d is ready (deps satisfied)\n", job_id);
                }
            }
        }

        if (ready_jobs.empty()) {
            // No jobs ready but not all completed - shouldn't happen unless there's a cycle
            debug_print("ERROR: No ready jobs but %zu jobs remain\n", total_jobs - jobs_completed);
            break;
        }

        // Add all ready jobs to the runtime for parallel execution
        debug_print("Main thread: Adding %zu ready jobs to runtime\n", ready_jobs.size());
        m_runtime.add_jobs(ready_jobs);

        // Wait for all jobs in this level to complete
        {
            std::unique_lock<std::mutex> lock(m_completed_mutex);
            m_completed_cv.wait(lock, [&]{ return m_runtime.is_all_complete(); });
        }

        // Mark all completed jobs and update counter
        for (const auto& job : ready_jobs) {
            m_dep_graph.mark_completed(job.job_id);
            completed[job.job_id] = true;
            jobs_completed++;
            debug_print("Main thread: Job %d completed\n", job.job_id);
        }
    }

    debug_print("Main thread: All %zu jobs completed\n", total_jobs);
    return;
}
