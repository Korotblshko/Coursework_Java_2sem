package ru.coursework.rxjava;

public interface Emitter<T> {

    void onNext(T item);

    void onError(Throwable throwable);

    void onComplete();

    boolean isDisposed();
}