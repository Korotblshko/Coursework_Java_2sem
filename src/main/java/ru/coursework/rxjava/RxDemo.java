package ru.coursework.rxjava;

public class RxDemo {

    public static void main(String[] args) throws InterruptedException {
        Scheduler ioScheduler = new IOThreadScheduler();
        Scheduler singleScheduler = new SingleThreadScheduler();

        System.out.println("=== Demo 1: map + filter ===");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onNext(4);
                    emitter.onComplete();
                })
                .map(number -> number * 10)
                .filter(number -> number >= 20)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        System.out.println("[Observer] onNext: " + item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("[Observer] onError: " + throwable.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[Observer] onComplete");
                    }
                });

        System.out.println("=== Demo 2: flatMap ===");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(number -> Observable.<String>create(innerEmitter -> {
                    innerEmitter.onNext("item-" + number + "-A");
                    innerEmitter.onNext("item-" + number + "-B");
                    innerEmitter.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println("[FlatMapObserver] onNext: " + item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("[FlatMapObserver] onError: " + throwable.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[FlatMapObserver] onComplete");
                    }
                });

        System.out.println("=== Demo 3: subscribeOn + observeOn ===");

        Observable.<String>create(emitter -> {
                    System.out.println("[Source] thread: " + Thread.currentThread().getName());
                    emitter.onNext("Hello from source");
                    emitter.onComplete();
                })
                .subscribeOn(ioScheduler)
                .observeOn(singleScheduler)
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println("[Observer] item: " + item);
                        System.out.println("[Observer] thread: " + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("[Observer] error: " + throwable.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[Observer] complete");
                    }
                });

        System.out.println("=== Demo 4: error handling ===");

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(0);
                    emitter.onComplete();
                })
                .map(number -> 10 / number)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        System.out.println("[ErrorObserver] onNext: " + item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("[ErrorObserver] onError: " + throwable.getClass().getSimpleName());
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("[ErrorObserver] onComplete");
                    }
                });

        Thread.sleep(1000);

        ioScheduler.shutdown();
        singleScheduler.shutdown();
    }
}