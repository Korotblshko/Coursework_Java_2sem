package ru.coursework.rxjava;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ObservableTest {

    @Test
    void shouldEmitItemsAndComplete() {
        TestObserver<Integer> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(observer);

        assertEquals(List.of(1, 2), observer.getItems());
        assertTrue(observer.isCompleted());
        assertNull(observer.getError());
    }

    @Test
    void shouldMapItems() {
        TestObserver<Integer> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .map(item -> item * 10)
                .subscribe(observer);

        assertEquals(List.of(10, 20), observer.getItems());
        assertTrue(observer.isCompleted());
    }

    @Test
    void shouldFilterItems() {
        TestObserver<Integer> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .filter(item -> item % 2 == 0)
                .subscribe(observer);

        assertEquals(List.of(2, 4), observer.getItems());
        assertTrue(observer.isCompleted());
    }

    @Test
    void shouldFlatMapItems() {
        TestObserver<String> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(item -> Observable.<String>create(innerEmitter -> {
                    innerEmitter.onNext("item-" + item + "-A");
                    innerEmitter.onNext("item-" + item + "-B");
                    innerEmitter.onComplete();
                }))
                .subscribe(observer);

        assertEquals(
                List.of("item-1-A", "item-1-B", "item-2-A", "item-2-B"),
                observer.getItems()
        );
        assertTrue(observer.isCompleted());
    }

    @Test
    void shouldHandleErrorFromSource() {
        TestObserver<Integer> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            throw new IllegalStateException("source failed");
        }).subscribe(observer);

        assertEquals(List.of(1), observer.getItems());
        assertNotNull(observer.getError());
        assertEquals("source failed", observer.getError().getMessage());
        assertFalse(observer.isCompleted());
    }

    @Test
    void shouldHandleErrorFromMap() {
        TestObserver<Integer> observer = new TestObserver<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(0);
                    emitter.onComplete();
                })
                .map(item -> 10 / item)
                .subscribe(observer);

        assertEquals(List.of(10), observer.getItems());
        assertNotNull(observer.getError());
        assertTrue(observer.getError() instanceof ArithmeticException);
    }

    @Test
    void shouldStopEmittingAfterDispose() {
        TestObserver<Integer> observer = new TestObserver<>();

        final Disposable[] disposableHolder = new Disposable[1];

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            disposableHolder[0].dispose();
            emitter.onNext(2);
            emitter.onComplete();
        });

        disposableHolder[0] = observable.subscribe(observer);

        assertEquals(List.of(1), observer.getItems());
        assertFalse(observer.isCompleted());
    }
}