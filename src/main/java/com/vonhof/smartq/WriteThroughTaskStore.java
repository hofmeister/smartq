package com.vonhof.smartq;


import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class WriteThroughTaskStore implements TaskStore {
    private static final Logger log = Logger.getLogger(WriteThroughTaskStore.class);

    private final MemoryTaskStore memStore;
    private final PostgresTaskStore diskStore;
    private final LinkedList<Runnable> tasks = new LinkedList<>();
    private final WorkerQueue workerQueue = new WorkerQueue();
    private volatile boolean closed = false;
    private volatile Exception lastException;

    public WriteThroughTaskStore(PostgresTaskStore diskStore) {
        this.memStore = new MemoryTaskStore();
        this.diskStore = diskStore;
        workerQueue.start();
        reload();
    }


    /**
     * Resyncs the mem store with the disk store.
     */
    public void reload() {
        try {
            memStore.resetQueues();
        } catch (InterruptedException e) {
            return;
        }

        Iterator<Task> queued = diskStore.getQueued();
        List<Task> tasks = new LinkedList<>();
        while(queued.hasNext()) {
            tasks.add(queued.next());
        }
        memStore.queue(tasks.toArray(new Task[tasks.size()]));

        for(String tag : memStore.getTags()) {
            memStore.setTaskTypeEstimate(tag,diskStore.getTaskTypeEstimate(tag));
        }
    }

    @Override
    public Task get(UUID id) {
        return memStore.get(id);
    }

    @Override
    public void remove(final Task task) {
        memStore.remove(task);
        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.remove(task);
            }
        });
    }

    @Override
    public void remove(final UUID id) {
        memStore.remove(id);

        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.remove(id);
            }
        });
    }

    @Override
    public void queue(final Task... tasks) {
        memStore.queue(tasks);

        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.queue(tasks);
            }
        });
    }

    @Override
    public void run(final Task task) {
        memStore.run(task);

        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.run(task);
            }
        });

    }

    @Override
    public void failed(final Task task) {
        memStore.failed(task);

        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.failed(task);
            }
        });
    }

    @Override
    public Iterator<Task> getFailed() {
        return memStore.getFailed();
    }

    @Override
    public Iterator<Task> getQueued() {
        return memStore.getQueued();
    }

    @Override
    public Iterator<Task> getQueued(String type) {
        return memStore.getQueued(type);
    }

    @Override
    public Iterator<UUID> getQueuedIds() {
        return memStore.getQueuedIds();
    }

    @Override
    public Iterator<UUID> getQueuedIds(String type) {
        return memStore.getQueuedIds(type);
    }

    @Override
    public Iterator<Task> getRunning() {
        return memStore.getRunning();
    }

    @Override
    public Iterator<Task> getRunning(String type) {
        return memStore.getRunning(type);
    }

    @Override
    public long queueSize() throws InterruptedException {
        return memStore.queueSize();
    }

    @Override
    public long runningCount() throws InterruptedException {
        return memStore.runningCount();
    }

    @Override
    public long queueSize(String type) throws InterruptedException {
        return memStore.queueSize(type);
    }

    @Override
    public long runningCount(String type) throws InterruptedException {
        return memStore.runningCount(type);
    }

    @Override
    public Set<String> getTags() throws InterruptedException {
        return memStore.getTags();
    }

    @Override
    public <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
        return diskStore.isolatedChange(callable);
    }

    @Override
    public void waitForChange() throws InterruptedException {
        diskStore.waitForChange();
    }

    @Override
    public void signalChange() {
        diskStore.signalChange();
    }

    @Override
    public ParallelIterator<Task> getPending() {
        return memStore.getPending();
    }

    @Override
    public ParallelIterator<Task> getPending(String tag) {
        return memStore.getPending(tag);
    }

    @Override
    public long getTaskTypeEstimate(String type) {
        return memStore.getTaskTypeEstimate(type);
    }

    @Override
    public void addTaskTypeDuration(final String type, final long duration) {
        memStore.addTaskTypeDuration(type, duration);
        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.addTaskTypeDuration(type, duration);
            }
        });
    }

    @Override
    public void setTaskTypeEstimate(final String type, final long estimate) {
        memStore.setTaskTypeEstimate(type, estimate);
        doLater(new Runnable() {
            @Override
            public void run() {
                diskStore.setTaskTypeEstimate(type, estimate);
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (workerQueue.isAlive()) {
            waitForAsyncTasks();
            closed = true;
            workerQueue.interrupt();
            workerQueue.join();
        }

        diskStore.close();
        memStore.close();

        if (lastException != null) {
            throw lastException;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }

    @Override
    public Task getFirstTaskWithReference(String referenceId) {
        return memStore.getFirstTaskWithReference(referenceId);
    }

    @Override
    public Task getLastTaskWithReference(String referenceId) {
        return memStore.getLastTaskWithReference(referenceId);
    }

    private void doLater(final Runnable runnable) {
        if (closed) {
            throw new RuntimeException("Cannot add new tasks to a closed store");
        }
        synchronized (tasks) {
            tasks.addFirst(new Runnable() {
                @Override
                public void run() {
                    synchronized (diskStore) {
                        runnable.run();
                    }
                }
            });

            tasks.notifyAll();
        }
    }

    public void waitForAsyncTasks() throws InterruptedException {
        while(!tasks.isEmpty()) {
            synchronized (tasks) {
                tasks.wait(5000);
            }
        }
    }

    private final class WorkerQueue extends Thread {
        private WorkerQueue() {
            super("write-through-queue");
        }

        @Override
        public void run() {
            while(!tasks.isEmpty() || (!interrupted() && !closed)) {

                while(!tasks.isEmpty()) {
                    Runnable task = null;
                    synchronized (tasks) {
                        task = tasks.pollLast();
                    }

                    try {
                        task.run();
                    } catch (Exception e) {
                        lastException = e;
                        log.error("Async task failed", e);
                        return;
                    }
                }

                try {
                    synchronized (tasks) {
                        tasks.wait(5000);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }

            try {
                tasks.clear();
                diskStore.close();
            } catch (Exception e) {
                log.warn("Failed to close disk store", e);
            }
        }
    }
}
