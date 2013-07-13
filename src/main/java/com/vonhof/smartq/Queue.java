package com.vonhof.smartq;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Queue<T extends Task,U extends Serializable>  {

    private final Map<String, Integer> taskTypeRateLimits = new ConcurrentHashMap<String, Integer>();

    private volatile int consumers = 0;

    protected abstract CountMap<String> getTypeETA();

    protected abstract long getTypeETA(String taskType);

    public int getConsumers() {
        return consumers;
    }

    public void setConsumers(int consumers) {
        this.consumers = consumers;
    }

    protected int getConcurrency() {
        int concurrency = getConsumers();
        if (concurrency < 1) {
            concurrency = 1;
        }
        return concurrency;
    }

    protected int getConcurrency(String taskType) {
        int concurrency = getConcurrency();

        int concurrencyLimit = getTaskTypeRateLimit(taskType);
        int typeConcurrency = 1;
        if (concurrencyLimit < 0) {
            return concurrency;
        } else {
            return Math.min(concurrencyLimit,concurrency);
        }
    }

    /**
     * Limit the throughput of a specific task type ( e.g. how many tasks of the given type that may be processed
     * concurrently )
     * @param taskType
     * @param limit
     */
    public final void setTaskTypeRateLimit(String taskType, int limit) {
        taskTypeRateLimits.put(taskType, limit);
    }

    /**
     * Gets the max allowed concurrent tasks for a given task type. Returns -1 if no limit is specified.
     * @param taskType
     * @return
     */
    public final int getTaskTypeRateLimit(String taskType) {
        if (taskTypeRateLimits.containsKey(taskType)) {
            return taskTypeRateLimits.get(taskType);
        }
        return -1;
    }

    /**
     * Get estimated time untill queue no longer is blocked for the given task type.
     * @param taskType
     * @return
     */
    public long getEstimatedTimeLeft(String taskType) {
        final int concurrency = getConcurrency(taskType);

        long typeTimeLeftTotal = getTypeETA(taskType);

        return (long)Math.ceil((double)typeTimeLeftTotal / (double)concurrency);
    }


    /**
     * Get estimated time until queue no longer is blocked
     * @return
     */
    public long getEstimatedTimeLeft() {

        long timeLeft = 0;

        final CountMap<String> typeETA = getTypeETA();

        for(Map.Entry<String, Long> entry:typeETA.entrySet()) {
            String taskType = entry.getKey();

            int typeConcurrency = getConcurrency(taskType);

            long typeTimeLeftTotal = entry.getValue();

            timeLeft += (long)Math.ceil((double)typeTimeLeftTotal / (double)typeConcurrency);
        }

        return timeLeft;
    }

    /**
     * Returns the amount of tasks in this queue.
     * @return
     */
    public abstract long size();

    /**
     * Submits a task for processing
     * @param task
     * @return
     */
    public abstract boolean submit(T task);

    /**
     * Removes a task, will return false if task is being processed.
     * @param task
     * @return
     */
    public abstract boolean cancel(T task);

    /**
     * Acknowledge a task. Removes it from queue.
     * @param id
     */
    public abstract void acknowledge(UUID id,U response);

    public void acknowledge(UUID id) {
        acknowledge(id,null);
    }


    /**
     * Acquire next task, blocks if nothing is available, or if rate limiter determines no tasks are eligible for
     * execution.
     * @return
     */
    public abstract T acquire(String taskType) throws InterruptedException;

    public T acquire() throws InterruptedException {
       return acquire(null);
    }
}
