package com.vonhof.smartq;


import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public interface TaskStore<T extends Task> {

    public T get(UUID id);

    public void remove(T task);

    public void remove(UUID id);

    public void queue(T task);

    public void run(T task);

    public Iterator<T> getQueued();

    public Iterator<T> getQueued(String type);

    public Iterator<T> getRunning();

    public Iterator<T> getRunning(String type);

    public long queueSize();

    public long runningCount();

    public long queueSize(String type);

    public long runningCount(String type);

    public Set<String> getTypes();

    public long getQueuedETA(String type);

    public long getQueuedETA();

    public <U> U isolatedChange(Callable<U> callable) throws InterruptedException;

    public void waitForChange() throws InterruptedException;

    public void signalChange();
}
