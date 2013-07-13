package com.vonhof.smartq;

import java.io.Serializable;
import java.util.*;


public class DefaultQueue<T extends Task,U extends Serializable> extends Queue<T,U> {

    private final TaskStore<T> store;

    public DefaultQueue(TaskStore<T> store) {
        this.store = store;
    }

    @Override
    protected CountMap<String> getTypeETA() {
        CountMap<String> typeETA = new CountMap<String>();

        for(T task: store.getRunning()) {
            typeETA.increment(task.getType(), task.getEstimatedTimeLeft());
        }

        for(T task: store.getQueued()) {
            typeETA.increment(task.getType(),task.getEstimatedTimeLeft());
        }

        return typeETA;
    }

    @Override
    protected long getTypeETA(String type) {
        return getTypeETA().get(type);
    }


    @Override
    public long size() {
        return store.queueSize();
    }

    @Override
    public boolean submit(T task) {

        synchronized (store) {
            store.queue(task);
            store.notifyAll();
        }

        return true;
    }

    @Override
    public synchronized boolean cancel(T task) {
        if (store.get(task.getId()).isRunning()) {
            return false;
        }

        store.remove(task);

        return true;
    }

    @Override
    public synchronized void acknowledge(UUID id, U response) {
        T task = store.get(id);
        if (task == null) {
            throw new IllegalArgumentException("Task not found");
        }

        task.setState(Task.State.DONE);
        task.setEnded(WatchProvider.currentTime());
        store.remove(task);
    }

    @Override
    public T acquire(String taskType) throws InterruptedException {

        T selectedTask = null;

        while(selectedTask == null) {
            CountMap<String> tasksRunning = new CountMap<String>();

            synchronized (this) {
                for(T task : store.getRunning()) {

                    tasksRunning.increment(task.getType(), 1);
                }
            }

            synchronized (store) {
                for(T task : store.getQueued()) {
                    if (taskType != null && !task.getType().equals(taskType)) {
                        continue;
                    }

                    int limit = getTaskTypeRateLimit(task.getType());

                    if (limit > 0) {
                        long running = tasksRunning.get(task.getType());

                        if (running >= limit) {
                            continue;
                        }
                    }

                    selectedTask = task;
                }

                if (selectedTask == null) {
                    store.wait();
                }  else {
                    break;
                }
            }
        }

        synchronized (this) {
            if (selectedTask != null) {
                selectedTask.setState(Task.State.RUNNING);
                selectedTask.setStarted(WatchProvider.currentTime());
                store.run(selectedTask);
            }
        }

        return selectedTask;
    }
}
