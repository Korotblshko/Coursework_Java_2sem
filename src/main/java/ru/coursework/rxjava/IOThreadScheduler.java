package ru.coursework.rxjava;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IOThreadScheduler implements Scheduler {

    private final ExecutorService executorService = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("rx-io-" + thread.getId());
        return thread;
    });

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}