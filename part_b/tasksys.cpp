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
        while (true) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(m_runtime.m_queue_mutex);
                m_runtime.m_task_cv.wait(lock, [&]{
                    return !m_runtime.m_tasks.empty() || m_done.load();
                });

                if (m_done.load() && m_runtime.m_tasks.empty()) {
                    break;
                }

                if (!m_runtime.m_tasks.empty()) {
                    task = m_runtime.m_tasks.front();
                    m_runtime.m_tasks.pop();
                }
            }

            if (task.runnable != nullptr) {
                task.runnable->runTask(task.task_id, task.total_tasks);
                m_runtime.add_complete();

                // Check if this was the last task of a job (for dependency-driven execution)
                if (task.job_id >= 0 && m_runtime.m_dep_graph != nullptr) {
                    Job* current_job = m_dep_graph.get_job(task.job_id);
                    int remaining = current_job->tasks_remaining.fetch_sub(1) - 1;

                    if (remaining == 0) {
                        // This job is complete - process dependents
                        debug_print("Worker %d: Job %d completed, checking dependents\n", thread_id, task.job_id);

                        const std::vector<TaskID>& dependents = m_dep_graph.get_dependents(task.job_id);

                        std::vector<Job*> newly_ready;
                        for (TaskID dep_id : dependents) {
                            Job* dependent_job = m_dep_graph.get_job(dep_id);
                            if (dependent_job != nullptr) {
                                int deps_remaining = dependent_job->remaining_deps.fetch_sub(1) - 1;
                                if (deps_remaining == 0) {
                                    // This dependent is now ready
                                    newly_ready.push_back(dependent_job);
                                    debug_print("Worker %d: Job %d is now ready\n", thread_id, dep_id);
                                }
                            }
                        }

                        // Add tasks for newly ready jobs
                        if (!newly_ready.empty()) {
                            int total_new_tasks = 0;

                            // Add all tasks at once while holding the lock
                            {
                                std::lock_guard<std::mutex> lock(m_runtime.m_queue_mutex);
                                for (Job* job : newly_ready) {
                                    for (int i = 0; i < job->total_tasks; i++) {
                                        Task new_task;
                                        new_task.runnable = job->runnable;
                                        new_task.task_id = i;
                                        new_task.total_tasks = job->total_tasks;
                                        new_task.job_id = job->job_id;
                                        m_runtime.m_tasks.push(new_task);
                                        total_new_tasks++;
                                    }
                                }
                            }

                            // Wake up workers proportional to tasks added
                            if (total_new_tasks > 1) {
                                m_runtime.m_task_cv.notify_all();
                            } else {
                                m_runtime.m_task_cv.notify_one();
                            }
                        }

                        // Increment jobs completed counter
                        size_t completed = m_runtime.m_jobs_completed.fetch_add(1) + 1;
                        debug_print("Worker %d: Jobs completed: %zu / %zu\n", thread_id, completed, m_runtime.m_total_jobs);

                        // Notify main thread if all jobs complete (for dependency-driven execution)
                        if (m_runtime.all_jobs_complete()) {
                            m_completed_cv.notify_one();
                        }
                    }
                }

                // Notify main thread if all tasks complete (for simple run())
                if (m_runtime.m_dep_graph == nullptr && m_runtime.is_all_complete()) {
                    m_completed_cv.notify_one();
                }
            }
            else 
            {
                std::this_thread::yield();
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
    m_runtime.m_task_cv.notify_all();  // Wake up all workers
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

    return m_dep_graph.add_job(runnable, num_total_tasks, deps);
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // Pipelined execution: workers handle dependency resolution,
    // main thread just initializes and waits once
    //

    size_t total_jobs = m_dep_graph.size();
    if (total_jobs == 0) return;

    debug_print("Main thread: Starting sync with %zu jobs\n", total_jobs);

    // Set up dependency graph pointer for workers
    m_runtime.set_dep_graph(&m_dep_graph);

    // Initialize job tracking
    m_runtime.init_job_tracking(total_jobs);

    // Find initially ready jobs (jobs with no dependencies)
    std::vector<Job*> initial_ready;
    for (size_t job_id = 0; job_id < total_jobs; job_id++) {
        Job* job = m_dep_graph.get_job(job_id);
        if (job != nullptr && job->remaining_deps.load() == 0) {
            initial_ready.push_back(job);
            debug_print("Main thread: Job %d initially ready (no deps)\n", job_id);
        }
    }

    // Add initially ready jobs to the queue
    if (!initial_ready.empty()) {
        std::lock_guard<std::mutex> lock(m_runtime.m_queue_mutex);
        for (Job* job : initial_ready) {
            for (int i = 0; i < job->total_tasks; i++) {
                Task task;
                task.runnable = job->runnable;
                task.task_id = i;
                task.total_tasks = job->total_tasks;
                task.job_id = job->job_id;
                m_runtime.m_tasks.push(task);
            }
        }
        m_runtime.m_task_cv.notify_all();
        debug_print("Main thread: Added %zu initial jobs to queue\n", initial_ready.size());
    }

    // Wait once for all jobs to complete (workers handle dependency resolution)
    {
        std::unique_lock<std::mutex> lock(m_completed_mutex);
        m_completed_cv.wait(lock, [&]{ return m_runtime.all_jobs_complete(); });
    }

    debug_print("Main thread: All %zu jobs completed\n", total_jobs);

    // Clean up
    m_runtime.set_dep_graph(nullptr);
    return;
}
