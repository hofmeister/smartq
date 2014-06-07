package com.vonhof.smartq;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public interface TaskStore<T extends Task> {

    public T get(UUID id);

    public void remove(T task);

    public void remove(UUID id);

    public void queue(T ... tasks);

    public void run(T task);

    public void failed(T task);

    public Iterator<T> getFailed();

    public Iterator<T> getQueued();

    public Iterator<T> getQueued(String type);

    public Iterator<UUID> getQueuedIds();

    public Iterator<UUID> getQueuedIds(String type);

    public Iterator<T> getRunning();

    public Iterator<T> getRunning(String type);

    public long queueSize() throws InterruptedException;

    public long runningCount() throws InterruptedException;

    public long queueSize(String type) throws InterruptedException;

    public long runningCount(String type) throws InterruptedException;

    public Set<String> getTags() throws InterruptedException;

    public <U> U isolatedChange(Callable<U> callable) throws InterruptedException;

    public void waitForChange() throws InterruptedException;

    public void signalChange();

    Iterator<T> getPending();

    Iterator<T> getPending(String tag);

    long getTaskTypeEstimate(String type);

    void addTaskTypeDuration(String type, long duration);

    void setTaskTypeEstimate(String type, long estimate);

    void close() throws Exception;
}
