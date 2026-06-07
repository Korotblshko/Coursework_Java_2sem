package ru.coursework.rxjava;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SingleThreadScheduler implements Scheduler {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("rx-single-" + thread.getId());
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