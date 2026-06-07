package ru.coursework.threadpool;

public class TaskWrapper implements Runnable {

    private final Runnable task;
    private final String description;

    public TaskWrapper(Runnable task, String description) {
        this.task = task;
        this.description = description;
    }

    @Override
    public void run() {
        task.run();
    }

    public String getDescription() {
        return description;
    }
}