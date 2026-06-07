package ru.coursework.rxjava;

@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(Emitter<T> emitter) throws Exception;
}