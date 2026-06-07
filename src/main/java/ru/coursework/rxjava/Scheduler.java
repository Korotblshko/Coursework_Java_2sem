package ru.coursework.rxjava;

public interface Scheduler {

    void execute(Runnable task);

    void shutdown();
}