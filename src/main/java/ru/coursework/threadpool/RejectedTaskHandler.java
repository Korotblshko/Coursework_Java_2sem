package ru.coursework.threadpool;

public interface RejectedTaskHandler {

    void rejected(TaskWrapper task, CustomThreadPool pool);
}