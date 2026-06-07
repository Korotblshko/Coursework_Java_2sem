package ru.coursework.threadpool;

import java.util.concurrent.RejectedExecutionException;

public class AbortRejectedTaskHandler implements RejectedTaskHandler {

    @Override
    public void rejected(TaskWrapper task, CustomThreadPool pool) {
        System.out.println("[Rejected] Task " + task.getDescription() + " was rejected due to overload!");
        throw new RejectedExecutionException("Task rejected due to overload: " + task.getDescription());
    }
}