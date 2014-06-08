package com.vonhof.smartq;


import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class WriteThroughTaskStore implements TaskStore {
    private static final Logger log = Logger.getLogger(WriteThroughTaskStore.class);


    private final ExecutorService laterQueue = Executors.newSingleThreadExecutor();
    private final MemoryTaskStore memStore;
    private final TaskStore diskStore;
    private final Stack<Future> futures = new Stack<>();
    private final Timer timer = new Timer();

    public WriteThroughTaskStore(TaskStore diskStore) {
        this.memStore = new MemoryTaskStore();
        this.diskStore = diskStore;

        reload();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Iterator<Future> it = futures.iterator();
                while(it.hasNext()) {
                    Future next = it.next();
                    if (next.isDone() || next.isCancelled()) {
                        it.remove();
                    }
                }
            }
        }, 0, 1000L * 60L * 10L);
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
        memStore.queue((Task[]) tasks.toArray(new Task[tasks.size()]));

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
        diskStore.remove(task);
    }

    @Override
    public void remove(final UUID id) {
        memStore.remove(id);
        diskStore.remove(id);
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
        diskStore.run(task);

    }

    @Override
    public void failed(final Task task) {
        memStore.failed(task);
        diskStore.failed(task);
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
        memStore.close();
        diskStore.close();
        timer.cancel();
    }

    @Override
    public Task getFirstTaskWithReference(String referenceId) {
        return memStore.getFirstTaskWithReference(referenceId);
    }

    @Override
    public Task getLastTaskWithReference(String referenceId) {
        return memStore.getLastTaskWithReference(referenceId);
    }

    private Future<?> doLater(final Runnable runnable) {
        final Future<?> future = laterQueue.submit(new Runnable() {
            @Override
            public void run() {
                synchronized (diskStore) {
                    runnable.run();
                }
            }
        });
        futures.push(future);
        return future;
    }

    public void waitForAsyncTasks() throws InterruptedException {
        while(!futures.isEmpty()) {
            try {
                futures.pop().get();
            } catch (ExecutionException e) {
                log.error("Async task failed", e);
            }
        }
    }
}
