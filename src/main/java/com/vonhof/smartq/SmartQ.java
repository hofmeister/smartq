package com.vonhof.smartq;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SmartQ<T extends Task,U extends Serializable>  {

    private static final Logger log = Logger.getLogger(SmartQ.class);

    private final Map<String, Integer> taskTypeRateLimits = new ConcurrentHashMap<String, Integer>();

    private volatile int consumers = 0;

    public SmartQ(TaskStore<T> store) {
        this.store = store;
    }

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


    private final TaskStore<T> store;



    
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

    
    protected long getTypeETA(String type) {
        return getTypeETA().get(type);
    }


    
    public long size() {
        return store.queueSize();
    }

    
    public boolean submit(T task) {

        store.queue(task);
        store.signalChange();

        return true;
    }

    
    public synchronized boolean cancel(T task) {
        if (store.get(task.getId()).isRunning()) {
            return false;
        }

        log.debug("Cancelled task");
        store.remove(task);
        store.signalChange();

        return true;
    }

    
    public synchronized void acknowledge(UUID id, U response) {
        T task = store.get(id);
        if (task == null) {
            throw new IllegalArgumentException("Task not found");
        }

        task.setState(Task.State.DONE);
        task.setEnded(WatchProvider.currentTime());
        synchronized (store) {
            log.debug("Acked task");
            store.remove(task);
            store.signalChange();
        }
    }

    
    public T acquire(String taskType) throws InterruptedException {
        store.lock();

        try {
            T selectedTask = null;

            while(selectedTask == null) {
                CountMap<String> tasksRunning = new CountMap<String>();
                log.debug(String.format("Running tasks: %s", store.runningCount()));

                for(T task : store.getRunning()) {
                    tasksRunning.increment(task.getType(), 1);
                }

                log.debug(String.format("Queue size: %s", store.queueSize()));

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
                    log.debug("Waiting for tasks");
                    store.waitForChange();
                    log.debug("Woke up!");
                }  else {
                    log.debug("Acquired task");
                    break;
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
        } finally {
            log.debug("Unlocking queue");
            store.unlock();
        }
    }


    public void acknowledge(UUID id) {
        acknowledge(id,null);
    }


    public T acquire() throws InterruptedException {
       return acquire(null);
    }
}
