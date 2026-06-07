package ru.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerTest {

    @Test
    void subscribeOnShouldRunSourceInSchedulerThread() throws InterruptedException {
        Scheduler scheduler = new IOThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> sourceThreadName = new AtomicReference<>();

        Observable.<String>create(emitter -> {
                    sourceThreadName.set(Thread.currentThread().getName());
                    emitter.onNext("data");
                    emitter.onComplete();
                })
                .subscribeOn(scheduler)
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotNull(sourceThreadName.get());
        assertTrue(sourceThreadName.get().startsWith("rx-io-"));

        scheduler.shutdown();
    }

    @Test
    void observeOnShouldRunObserverInSchedulerThread() throws InterruptedException {
        Scheduler scheduler = new SingleThreadScheduler();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> observerThreadName = new AtomicReference<>();

        Observable.<String>create(emitter -> {
                    emitter.onNext("data");
                    emitter.onComplete();
                })
                .observeOn(scheduler)
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        observerThreadName.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotNull(observerThreadName.get());
        assertTrue(observerThreadName.get().startsWith("rx-single-"));

        scheduler.shutdown();
    }

    @Test
    void computationSchedulerShouldExecuteTask() throws InterruptedException {
        Scheduler scheduler = new ComputationScheduler(2);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        scheduler.execute(() -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
        });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotNull(threadName.get());
        assertTrue(threadName.get().startsWith("rx-computation-"));

        scheduler.shutdown();
    }
}