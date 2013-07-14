package com.vonhof.smartq;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SmartQ<T extends Task,U extends Serializable>  {

    private static final Logger log = Logger.getLogger(SmartQ.class);

    private final Map<String, Integer> taskTypeRateLimits = new ConcurrentHashMap<String, Integer>();

    private volatile int consumers = 0;
    private final TaskStore<T> store;

    private List<QueueListener> listeners = new ArrayList<QueueListener>();

    public SmartQ(final TaskStore<T> store) {
        this.store = store;
    }


    public int getConsumers() {
        return consumers;
    }

    public TaskStore<T> getStore() {
        return store;
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

    public void addListener(QueueListener listener) {
        listeners.add(listener);
    }

    private void triggerAcquire(T task) {
        for(QueueListener listener : listeners) {
            listener.onAcquire(task);
        }
    }

    private void triggerSubmit(T task) {
        for(QueueListener listener : listeners) {
            listener.onSubmit(task);
        }
    }

    private void triggerDone(T task) {
        for(QueueListener listener : listeners) {
            listener.onDone(task);
        }
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
    public long getEstimatedTimeLeft(String taskType) throws InterruptedException {
        final int concurrency = getConcurrency(taskType);

        long typeTimeLeftTotal = getTypeETA(taskType);

        return (long)Math.ceil((double)typeTimeLeftTotal / (double)concurrency);
    }


    /**
     * Get estimated time until queue no longer is blocked
     * @return
     */
    public long getEstimatedTimeLeft() throws InterruptedException {

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
    
    protected CountMap<String> getTypeETA() throws InterruptedException {
        CountMap<String> typeETA = new CountMap<String>();

        getStore().lock();

        try {
            for(T task: getStore().getRunning()) {
                typeETA.increment(task.getType(), task.getEstimatedTimeLeft());
            }

            for(T task: getStore().getQueued()) {
                typeETA.increment(task.getType(),task.getEstimatedTimeLeft());
            }
        } finally {
            getStore().unlock();
        }

        return typeETA;
    }


    protected long getTypeETA(String type) throws InterruptedException {
        return getTypeETA().get(type);
    }

    /**
     * Amount of running task
     * @return
     */
    public long runningCount() {
        return getStore().runningCount();
    }


    /**
     * Amount of queued task
     * @return
     */
    public long queueSize() {
        return getStore().queueSize();
    }

    /**
     * Returns total size of queue + currently running tasks
     * @return
     */
    public long size() {
        return getStore().queueSize() + getStore().runningCount();
    }

    
    public boolean submit(T task) throws InterruptedException {

        getStore().lock();
        try {
            getStore().queue(task);
            getStore().signalChange();

            triggerSubmit(task);
        } finally {
            getStore().unlock();
        }


        return true;
    }

    
    public synchronized boolean cancel(T task) throws InterruptedException {
        if (getStore().get(task.getId()).isRunning()) {
            return false;
        }

        getStore().lock();

        try {
            log.debug("Cancelled task");
            getStore().remove(task);
            getStore().signalChange();

            triggerDone(task);
        } finally {
            getStore().unlock();
        }

        return true;
    }

    
    public synchronized void acknowledge(UUID id, U response) throws InterruptedException {
            T task = getStore().get(id);
            if (task == null) {
                throw new IllegalArgumentException("Task not found: " + id);
            }

        getStore().lock();

        try {
            task.setState(Task.State.DONE);
            task.setEnded(WatchProvider.currentTime());

            log.debug("Acked task");
            getStore().remove(task);
            getStore().signalChange();

            triggerDone(task);
        } finally {
            getStore().unlock();
        }
    }

    
    public T acquire(String taskType) throws InterruptedException {

            T selectedTask = null;

            while(selectedTask == null) {

                getStore().lock();

                CountMap<String> tasksRunning = new CountMap<String>();
                log.debug(String.format("Running tasks: %s", getStore().runningCount()));

                try {

                    for(T task : getStore().getRunning()) {
                        tasksRunning.increment(task.getType(), 1);
                    }

                    log.debug(String.format("Queue queueSize: %s", getStore().queueSize()));

                    for(T task : getStore().getQueued()) {
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
                        break;
                    }

                    if (selectedTask != null) {

                        selectedTask.setState(Task.State.RUNNING);
                        selectedTask.setStarted(WatchProvider.currentTime());
                        log.trace("Moving task to running pool");
                        getStore().run(selectedTask);
                        triggerAcquire(selectedTask);

                        log.debug("Acquired task");
                    }

                } finally {
                    getStore().unlock();
                }


                if (selectedTask == null) {
                    log.debug("Waiting for tasks");
                    getStore().waitForChange();
                    log.debug("Woke up!");
                }
            }

            return selectedTask;

    }


    public void acknowledge(UUID id) throws InterruptedException {
        acknowledge(id,null);
    }


    public T acquire() throws InterruptedException {
       return acquire(null);
    }
}
