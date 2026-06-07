package ru.coursework.threadpool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {

    private final CustomThreadPool pool;
    private final BlockingQueue<TaskWrapper> queue;
    private final int workerId;
    private final String workerName;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private volatile boolean running = true;
    private volatile boolean busy = false;
    private Thread thread;

    public Worker(
            CustomThreadPool pool,
            int workerId,
            String workerName,
            int queueSize,
            long keepAliveTime,
            TimeUnit timeUnit
    ) {
        this.pool = pool;
        this.workerId = workerId;
        this.workerName = workerName;
        this.queue = new LinkedBlockingQueue<>(queueSize);
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
    }

    @Override
    public void run() {
        this.thread = Thread.currentThread();

        try {
            while (running) {
                if (pool.isShutdownNow()) {
                    break;
                }

                if (pool.isShutdown() && queue.isEmpty()) {
                    break;
                }

                TaskWrapper task = queue.poll(keepAliveTime, timeUnit);

                if (task == null) {
                    if (pool.isShutdown() && queue.isEmpty()) {
                        break;
                    }

                    if (pool.canWorkerStop()) {
                        System.out.println("[Worker] " + getName() + " idle timeout, stopping.");
                        break;
                    }

                    continue;
                }

                if (pool.isShutdownNow()) {
                    break;
                }

                busy = true;

                try {
                    System.out.println("[Worker] " + getName() + " executes " + task.getDescription());
                    task.run();
                } catch (RuntimeException exception) {
                    System.out.println("[Worker] " + getName() + " task failed: " + exception.getMessage());
                } finally {
                    busy = false;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            pool.removeWorker(this);
            System.out.println("[Worker] " + getName() + " terminated.");
        }
    }

    public boolean offer(TaskWrapper task) {
        return queue.offer(task);
    }

    public int getQueueSize() {
        return queue.size();
    }

    public boolean isIdle() {
        return !busy && queue.isEmpty();
    }

    public void stopNow() {
        running = false;
        queue.clear();

        if (thread != null) {
            thread.interrupt();
        }
    }

    public String getName() {
        if (thread != null) {
            return thread.getName();
        }

        if (workerName != null) {
            return workerName;
        }

        return "worker-" + workerId;
    }
}