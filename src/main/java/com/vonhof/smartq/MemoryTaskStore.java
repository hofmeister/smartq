package com.vonhof.smartq;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class MemoryTaskStore<T extends Task> implements TaskStore<T> {
    private final Map<UUID, T> tasks = new ConcurrentHashMap<UUID, T>();
    private final List<T> queued = Collections.synchronizedList(new LinkedList<T>());
    private final List<T> running = Collections.synchronizedList(new LinkedList<T>());

    private final Lock lock = new ReentrantLock();


    @Override
    public T get(UUID id) {
        return tasks.get(id);
    }

    @Override
    public void remove(Task task) {
        tasks.remove(task.getId());
        queued.remove(task);
        running.remove(task);
    }

    @Override
    public void remove(UUID id) {
        tasks.remove(id);
    }

    @Override
    public void queue(T task) {
        tasks.put(task.getId(),task);
        queued.add(task);

        Collections.sort(queued,new Comparator<T>() {
            @Override
            public int compare(T t, T t2) {
                int diffPrio = t2.getPriority()-t.getPriority();
                if (diffPrio == 0) {
                    return (int) ( (t.getCreated() / 1000L) - (t2.getCreated() / 1000L) );
                }
                return diffPrio;
            }
        });
    }

    @Override
    public void run(T task) {
        queued.remove(task);
        running.add(task);
    }

    public List<T> getQueued() {
        return queued;
    }

    public List<T> getRunning() {
        return running;
    }

    @Override
    public long queueSize() {
        return queued.size();
    }

    @Override
    public long runningCount() {
        return running.size();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public synchronized void waitForChange() throws InterruptedException {
        this.wait();
    }

    @Override
    public synchronized void signalChange() {
        this.notifyAll();
    }
}
