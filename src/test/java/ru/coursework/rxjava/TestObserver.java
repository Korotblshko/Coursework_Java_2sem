package ru.coursework.rxjava;

import java.util.ArrayList;
import java.util.List;

public class TestObserver<T> implements Observer<T> {

    private final List<T> items = new ArrayList<>();
    private Throwable error;
    private boolean completed;

    @Override
    public void onNext(T item) {
        items.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
        this.error = throwable;
    }

    @Override
    public void onComplete() {
        this.completed = true;
    }

    public List<T> getItems() {
        return items;
    }

    public Throwable getError() {
        return error;
    }

    public boolean isCompleted() {
        return completed;
    }
}