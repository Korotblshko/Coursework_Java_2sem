package ru.coursework.threadpool;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                5,
                TimeUnit.SECONDS,
                5,
                1,
                "MyPool",
                new AbortRejectedTaskHandler()
        );

        System.out.println("=== Demo 1: normal task execution ===");

        for (int i = 1; i <= 8; i++) {
            int taskNumber = i;

            pool.execute("demo-task-" + taskNumber, () -> {
                System.out.println("[Task] demo-task-" + taskNumber + " started");
                sleep(1500);
                System.out.println("[Task] demo-task-" + taskNumber + " finished");
            });
        }

        System.out.println("=== Demo 2: Callable and Future ===");

        Future<String> future = pool.submit(() -> {
            System.out.println("[Callable] started");
            sleep(1000);
            return "Callable result";
        });

        System.out.println("[Main] Future result: " + future.get());

        System.out.println("=== Demo 3: overload and rejection ===");

        for (int i = 1; i <= 40; i++) {
            int taskNumber = i;

            try {
                pool.execute("overload-task-" + taskNumber, () -> {
                    System.out.println("[Task] overload-task-" + taskNumber + " started");
                    sleep(4000);
                    System.out.println("[Task] overload-task-" + taskNumber + " finished");
                });
            } catch (RejectedExecutionException exception) {
                System.out.println("[Main] Rejection caught: " + exception.getMessage());
            }
        }

        System.out.println("=== Waiting before shutdown ===");
        sleep(12000);

        System.out.println("=== Shutdown ===");
        pool.shutdown();

        boolean terminated = pool.awaitTermination(20, TimeUnit.SECONDS);

        System.out.println("[Main] Pool terminated: " + terminated);
        System.out.println("[Main] Worker count: " + pool.getWorkerCount());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}