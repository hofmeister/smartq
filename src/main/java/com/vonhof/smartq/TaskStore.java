package com.vonhof.smartq;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public interface TaskStore {

    public Task get(UUID id);

    public void remove(Task task);

    public void remove(UUID id);

    public void queue(Task ... tasks);

    public void run(Task task);

    public void failed(Task task);

    public Iterator<Task> getFailed();

    public Iterator<Task> getQueued();

    public Iterator<Task> getQueued(String type);

    public Iterator<UUID> getQueuedIds();

    public Iterator<UUID> getQueuedIds(String type);

    public Iterator<Task> getRunning();

    public Iterator<Task> getRunning(String type);

    public long queueSize() throws InterruptedException;

    public long runningCount() throws InterruptedException;

    public long queueSize(String type) throws InterruptedException;

    public long runningCount(String type) throws InterruptedException;

    public Set<String> getTags() throws InterruptedException;

    public <U> U isolatedChange(Callable<U> callable) throws InterruptedException;

    public void waitForChange() throws InterruptedException;

    public void signalChange();

    ParallelIterator<Task> getPending();

    ParallelIterator<Task> getPending(String tag);

    long getTaskTypeEstimate(String type);

    void addTaskTypeDuration(String type, long duration);

    void setTaskTypeEstimate(String type, long estimate);

    void close() throws Exception;
}
