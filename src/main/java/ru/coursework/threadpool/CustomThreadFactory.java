package ru.coursework.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {

    private final String poolName;
    private final AtomicInteger threadCounter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    public String nextThreadName() {
        return poolName + "-worker-" + threadCounter.getAndIncrement();
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = nextThreadName();
        return newThread(runnable, threadName);
    }

    public Thread newThread(Runnable runnable, String threadName) {
        System.out.println("[ThreadFactory] Creating new thread: " + threadName);

        return new Thread(() -> {
            try {
                runnable.run();
            } finally {
                System.out.println("[ThreadFactory] Thread finished: " + threadName);
            }
        }, threadName);
    }
}