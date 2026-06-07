package ru.coursework.rxjava;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    private final ObservableOnSubscribe<T> source;
    private Scheduler subscribeOnScheduler;
    private Scheduler observeOnScheduler;

    private Observable(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        return new Observable<>(source);
    }

    public Disposable subscribe(Observer<T> observer) {
        Objects.requireNonNull(observer, "observer cannot be null");

        SimpleDisposable disposable = new SimpleDisposable();
        SafeEmitter<T> emitter = new SafeEmitter<>(observer, disposable, observeOnScheduler);

        Runnable subscribeTask = () -> {
            try {
                if (!disposable.isDisposed()) {
                    source.subscribe(emitter);
                }
            } catch (Throwable throwable) {
                emitter.onError(throwable);
            }
        };

        if (subscribeOnScheduler != null) {
            subscribeOnScheduler.execute(subscribeTask);
        } else {
            subscribeTask.run();
        }

        return disposable;
    }

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper cannot be null");

        Observable<T> current = this;

        return new Observable<>(emitter -> current.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (emitter.isDisposed()) {
                    return;
                }

                try {
                    R result = mapper.apply(item);
                    emitter.onNext(result);
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate cannot be null");

        Observable<T> current = this;

        return new Observable<>(emitter -> current.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (emitter.isDisposed()) {
                    return;
                }

                try {
                    if (predicate.test(item)) {
                        emitter.onNext(item);
                    }
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public <R> Observable<R> flatMap(Function<? super T, Observable<R>> mapper) {
        Objects.requireNonNull(mapper, "mapper cannot be null");

        Observable<T> current = this;

        return new Observable<>(emitter -> current.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (emitter.isDisposed()) {
                    return;
                }

                try {
                    Observable<R> innerObservable = mapper.apply(item);

                    innerObservable.subscribe(new Observer<R>() {
                        @Override
                        public void onNext(R innerItem) {
                            emitter.onNext(innerItem);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            emitter.onError(throwable);
                        }

                        @Override
                        public void onComplete() {
                            // Завершение внутреннего Observable не завершает весь внешний поток.
                        }
                    });
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                emitter.onError(throwable);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler cannot be null");

        Observable<T> observable = new Observable<>(this.source);
        observable.subscribeOnScheduler = scheduler;
        observable.observeOnScheduler = this.observeOnScheduler;
        return observable;
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler cannot be null");

        Observable<T> observable = new Observable<>(this.source);
        observable.subscribeOnScheduler = this.subscribeOnScheduler;
        observable.observeOnScheduler = scheduler;
        return observable;
    }

    private static class SafeEmitter<T> implements Emitter<T> {

        private final Observer<T> observer;
        private final SimpleDisposable disposable;
        private final Scheduler observeOnScheduler;
        private boolean terminated = false;

        private SafeEmitter(Observer<T> observer, SimpleDisposable disposable, Scheduler observeOnScheduler) {
            this.observer = observer;
            this.disposable = disposable;
            this.observeOnScheduler = observeOnScheduler;
        }

        @Override
        public void onNext(T item) {
            if (terminated || disposable.isDisposed()) {
                return;
            }

            Runnable task = () -> {
                if (!disposable.isDisposed()) {
                    observer.onNext(item);
                }
            };

            dispatch(task);
        }

        @Override
        public void onError(Throwable throwable) {
            if (terminated || disposable.isDisposed()) {
                return;
            }

            terminated = true;

            Runnable task = () -> {
                if (!disposable.isDisposed()) {
                    observer.onError(throwable);
                }
                disposable.dispose();
            };

            dispatch(task);
        }

        @Override
        public void onComplete() {
            if (terminated || disposable.isDisposed()) {
                return;
            }

            terminated = true;

            Runnable task = () -> {
                if (!disposable.isDisposed()) {
                    observer.onComplete();
                }
                disposable.dispose();
            };

            dispatch(task);
        }

        @Override
        public boolean isDisposed() {
            return disposable.isDisposed();
        }

        private void dispatch(Runnable task) {
            if (observeOnScheduler != null) {
                observeOnScheduler.execute(task);
            } else {
                task.run();
            }
        }
    }
}