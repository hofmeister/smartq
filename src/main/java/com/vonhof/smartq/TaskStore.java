package com.vonhof.smartq;


import java.util.List;
import java.util.UUID;

public interface TaskStore<T extends Task> {

    public T get(UUID id);

    public void remove(T task);

    public void remove(UUID id);

    public void queue(T task);

    public void run(T task);

    public List<T> getQueued();

    public List<T> getRunning();

    public long queueSize();

    public long runningCount();
}
