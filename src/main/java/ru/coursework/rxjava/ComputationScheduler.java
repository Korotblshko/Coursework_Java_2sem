package ru.coursework.rxjava;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ComputationScheduler implements Scheduler {

    private final ExecutorService executorService;

    public ComputationScheduler() {
        int threads = Runtime.getRuntime().availableProcessors();
        this.executorService = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("rx-computation-" + thread.getId());
            return thread;
        });
    }

    public ComputationScheduler(int threads) {
        this.executorService = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("rx-computation-" + thread.getId());
            return thread;
        });
    }

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}