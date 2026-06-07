package ru.coursework.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final int minSpareThreads;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private final CustomThreadFactory threadFactory;
    private final RejectedTaskHandler rejectedTaskHandler;

    private final List<Worker> workers = new ArrayList<>();
    private final AtomicInteger workerIdCounter = new AtomicInteger(1);
    private final AtomicInteger taskIdCounter = new AtomicInteger(1);
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    private volatile boolean shutdown = false;
    private volatile boolean shutdownNow = false;

    public CustomThreadPool(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads,
            String poolName,
            RejectedTaskHandler rejectedTaskHandler
    ) {
        validateParameters(corePoolSize, maxPoolSize, keepAliveTime, queueSize, minSpareThreads);

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = new CustomThreadFactory(poolName);
        this.rejectedTaskHandler = rejectedTaskHandler;

        for (int i = 0; i < corePoolSize; i++) {
            createWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new IllegalArgumentException("Task cannot be null");
        }

        TaskWrapper task = new TaskWrapper(command, "task-" + taskIdCounter.getAndIncrement());
        executeWrapped(task);
    }

    public void execute(String description, Runnable command) {
        if (command == null) {
            throw new IllegalArgumentException("Task cannot be null");
        }

        TaskWrapper task = new TaskWrapper(command, description);
        executeWrapped(task);
    }

    private void executeWrapped(TaskWrapper task) {
        if (shutdown) {
            rejectedTaskHandler.rejected(task, this);
            return;
        }

        synchronized (this) {
            ensureSpareThreads();

            Worker targetWorker = chooseWorkerRoundRobin();

            if (targetWorker != null && targetWorker.offer(task)) {
                System.out.println("[Pool] Task accepted into queue of " + targetWorker.getName()
                        + ": " + task.getDescription());
                return;
            }

            Worker leastLoadedWorker = chooseLeastLoadedWorker();

            if (leastLoadedWorker != null && leastLoadedWorker.offer(task)) {
                System.out.println("[Pool] Task accepted into queue of " + leastLoadedWorker.getName()
                        + ": " + task.getDescription());
                return;
            }

            if (workers.size() < maxPoolSize) {
                Worker newWorker = createWorker();

                if (newWorker.offer(task)) {
                    System.out.println("[Pool] Task accepted into queue of " + newWorker.getName()
                            + ": " + task.getDescription());
                    return;
                }
            }
        }

        rejectedTaskHandler.rejected(task, this);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new IllegalArgumentException("Callable cannot be null");
        }

        FutureTask<T> futureTask = new FutureTask<>(callable);
        TaskWrapper task = new TaskWrapper(futureTask, "callable-task-" + taskIdCounter.getAndIncrement());
        executeWrapped(task);

        return futureTask;
    }

    @Override
    public void shutdown() {
        System.out.println("[Pool] Shutdown started. New tasks will not be accepted.");
        shutdown = true;
    }

    @Override
    public void shutdownNow() {
        System.out.println("[Pool] ShutdownNow started. Queues will be cleared and workers interrupted.");
        shutdown = true;
        shutdownNow = true;

        synchronized (this) {
            for (Worker worker : new ArrayList<>(workers)) {
                worker.stopNow();
            }
        }
    }

    public synchronized boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);

        while (!workers.isEmpty() && System.currentTimeMillis() < deadline) {
            wait(100);
        }

        return workers.isEmpty();
    }

    private Worker createWorker() {
        int workerId = workerIdCounter.getAndIncrement();
        String workerName = threadFactory.nextThreadName();

        Worker worker = new Worker(this, workerId, workerName, queueSize, keepAliveTime, timeUnit);

        workers.add(worker);

        Thread thread = threadFactory.newThread(worker, workerName);
        thread.start();

        return worker;
    }

    private void ensureSpareThreads() {
        int idleWorkers = countIdleWorkers();

        while (idleWorkers < minSpareThreads && workers.size() < maxPoolSize) {
            createWorker();
            idleWorkers++;
        }
    }

    private int countIdleWorkers() {
        int count = 0;

        for (Worker worker : workers) {
            if (worker.isIdle()) {
                count++;
            }
        }

        return count;
    }

    private Worker chooseWorkerRoundRobin() {
        if (workers.isEmpty()) {
            return null;
        }

        int index = Math.abs(roundRobinCounter.getAndIncrement() % workers.size());
        return workers.get(index);
    }

    private Worker chooseLeastLoadedWorker() {
        Worker result = null;

        for (Worker worker : workers) {
            if (result == null || worker.getQueueSize() < result.getQueueSize()) {
                result = worker;
            }
        }

        return result;
    }

    public synchronized boolean canWorkerStop() {
        return workers.size() > corePoolSize;
    }

    public synchronized void removeWorker(Worker worker) {
        workers.remove(worker);
        notifyAll();
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isShutdownNow() {
        return shutdownNow;
    }

    public synchronized int getWorkerCount() {
        return workers.size();
    }

    private void validateParameters(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            int queueSize,
            int minSpareThreads
    ) {
        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("corePoolSize must be greater than 0");
        }

        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be greater than or equal to corePoolSize");
        }

        if (keepAliveTime <= 0) {
            throw new IllegalArgumentException("keepAliveTime must be greater than 0");
        }

        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be greater than 0");
        }

        if (minSpareThreads < 0) {
            throw new IllegalArgumentException("minSpareThreads cannot be negative");
        }

        if (minSpareThreads > maxPoolSize) {
            throw new IllegalArgumentException("minSpareThreads cannot be greater than maxPoolSize");
        }
    }
}