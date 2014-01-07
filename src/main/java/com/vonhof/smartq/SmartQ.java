package com.vonhof.smartq;

import com.vonhof.smartq.Task.State;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class SmartQ<T extends Task,U extends Serializable>  {

    private static final Logger log = Logger.getLogger(SmartQ.class);

    private final Map<String, Integer> taskTypeRateLimits = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Integer> taskTypeRetryLimits = new ConcurrentHashMap<String, Integer>();

    private volatile int subscribers = 0;
    private final TaskStore<T> store;

    private final List<QueueListener> listeners = new ArrayList<QueueListener>();
    private boolean interrupted = false;

    public SmartQ(final TaskStore<T> store) {
        this.store = store;
    }


    public int getSubscribers() {
        return subscribers;
    }

    public TaskStore<T> getStore() {
        return store;
    }

    public void setSubscribers(int subscribers) {
        this.subscribers = subscribers;
    }

    protected int getConcurrency() {
        int concurrency = getSubscribers();
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

        int concurrencyLimit = getRateLimit(taskType);
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
    public final void setRateLimit(String taskType, int limit) {
        taskTypeRateLimits.put(taskType, limit);
    }

    /**
     * Gets the max allowed concurrent tasks for a given task type. Returns -1 if no limit is specified.
     * @param taskType
     * @return
     */
    public final int getRateLimit(String taskType) {
        if (taskTypeRateLimits.containsKey(taskType)) {
            return taskTypeRateLimits.get(taskType);
        }
        return -1;
    }

    public final void setMaxRetries(String taskType, int limit) {
        taskTypeRetryLimits.put(taskType, limit);
    }

    public final int getMaxRetries(String taskType) {
        if (taskTypeRetryLimits.containsKey(taskType)) {
            return taskTypeRetryLimits.get(taskType);
        }
        return -1;
    }

    /**
     * Get estimated time until queue no longer is blocked for the given task type.
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
        return getStore().isolatedChange(new Callable<CountMap<String>>() {
            @Override
            public CountMap<String> call() throws Exception {
                final CountMap<String> typeETA = new CountMap<String>();
                Iterator<T> running = getStore().getRunning();
                while(running.hasNext()) {
                    T task = running.next();
                    typeETA.increment(task.getType(), task.getEstimatedTimeLeft());
                }

                for(String type: getStore().getTypes()) {
                    typeETA.increment(type, getStore().getQueuedETA(type));
                }
                return typeETA;
            }
        });
    }


    protected long getTypeETA(String type) {
        long eta = getStore().getQueuedETA(type);
        for(T task: getRunningTasks(type)) {
            eta += task.getEstimatedTimeLeft();
        }
        return eta;
    }

    /**
     * Amount of running task
     * @return
     */
    public long runningCount() throws InterruptedException {
        return getStore().runningCount();
    }


    /**
     * Amount of queued task
     * @return
     */
    public long queueSize() throws InterruptedException {
        return getStore().queueSize();
    }

    /**
     * Returns total size of queue + currently running tasks
     * @return
     */
    public long size() throws InterruptedException {
        return getStore().queueSize() + getStore().runningCount();
    }

    
    public boolean submit(final T task) throws InterruptedException {

        getStore().isolatedChange(new Callable<T>() {
            @Override
            public T call() throws Exception {
                getStore().queue(task);
                getStore().signalChange();

                triggerSubmit(task);
                return task;
            }
        });

        return true;
    }

    public boolean cancel(UUID taskId) throws InterruptedException {
        return cancel(getStore().get(taskId));
    }

    public boolean cancel(T task) throws InterruptedException {
        return cancel(task, false);
    }

    public boolean cancel(UUID taskId, final boolean reschedule) throws InterruptedException {
        T t = getStore().get(taskId);
        return cancel(t, reschedule);

    }
    
    public boolean cancel(final T task, final boolean reschedule) throws InterruptedException {
        if (task == null) {
            return false;
        }

        getStore().isolatedChange(new Callable<T>() {
            @Override
            public T call() throws Exception {
                log.debug("Cancelled task");
                getStore().remove(task);
                task.setEnded(WatchProvider.currentTime());

                if (!reschedule) {
                    getStore().signalChange();
                    triggerDone(task);
                }
                return task;
            }
        });

        if (reschedule) {
            task.reset();
            submit(task);
        }

        return true;
    }

    
    public void acknowledge(final UUID id, U response) throws InterruptedException {
        getStore().isolatedChange(new Callable<T>() {

            @Override
            public T call() throws Exception {
                T task = getStore().get(id);
                if (task == null) {
                    throw new IllegalArgumentException("Task not found: " + id);
                }

                task.setState(Task.State.DONE);
                task.setEnded(WatchProvider.currentTime());

                log.debug("Acked task");
                getStore().remove(task);
                getStore().signalChange();

                triggerDone(task);
                return task;
            }
        });
    }

    public void failed(final UUID id) throws InterruptedException {

        getStore().isolatedChange(new Callable<T>() {

            @Override
            public T call() throws Exception {
                T task = getStore().get(id);
                if (task == null) {
                    throw new IllegalArgumentException("Task not found: " + id);
                }

                task.setState(State.ERROR);
                task.setEnded(WatchProvider.currentTime());

                log.debug("Task marked as failed");

                int maxRetries = getMaxRetries(task.getType());
                int attempts = task.getAttempts();

                if (maxRetries > attempts) {
                    getStore().remove(task);
                    task.reset();
                    task.setAttempts(attempts + 1);
                    submit(task);
                } else {
                    getStore().failed(task);
                    getStore().signalChange();
                    triggerDone(task);
                }

                return task;
            }
        });
    }

    public void interrupt() {
        interrupted = true;
        store.signalChange();
    }
    
    public T acquire(final String taskType) throws InterruptedException {

            interrupted = false;
            T selectedTask = null;

            while(selectedTask == null) {
                try {
                    selectedTask = getStore().isolatedChange(new Callable<T>() {
                        @Override
                        public T call() throws Exception {
                            CountMap<String> tasksRunning = new CountMap<String>();
                            log.debug(String.format("Running tasks: %s", getStore().runningCount()));

                            log.debug(String.format("Queue queueSize: %s", getStore().queueSize()));

                            Iterator<T> queued = taskType != null ? getStore().getQueued(taskType) : getStore().getQueued();
                            T taskLookup = null;
                            while(queued.hasNext()) {
                                final T task = queued.next();

                                int limit = getRateLimit(task.getType());

                                if (limit > 0) {
                                    if (!tasksRunning.contains(task.getType())) {
                                        tasksRunning.set(task.getType(), getStore().runningCount(task.getType()));
                                    }
                                    long running = tasksRunning.get(task.getType());

                                    if (running >= limit) {
                                        log.debug(String.format("Rate limited task type: %s (Running: %s, Limit: %s)",task.getType(), running, limit));
                                        continue;
                                    }
                                }

                                taskLookup = task;
                                break;
                            }

                            if (taskLookup != null) {
                                log.debug(String.format("Found task %s for type %s", taskLookup.getId(), taskType));
                                acquireTask(taskLookup);
                            } else {
                                log.debug(String.format("Found no tasks for type %s", taskType));
                            }

                            return taskLookup;
                        }
                    });

                } catch (Exception e) {
                    log.error("Failed while trying to get selected task", e);
                }


                if (selectedTask == null) {
                    log.debug("Waiting for tasks");
                    getStore().waitForChange();
                    log.debug("Woke up!");
                    if (interrupted) {
                        interrupted = false;
                        throw new AcquireInterruptedException();
                    }
                }
            }

            return selectedTask;
    }

    public List<T> getRunningTasks(String type) {
        List<T> out = new LinkedList<T>();
        Iterator<T> running = getStore().getRunning(type);
        while(running.hasNext()) {
            out.add(running.next());
        }

        return out;
    }


    public void acknowledge(UUID id) throws InterruptedException {
        acknowledge(id,null);
    }


    public T acquire() throws InterruptedException {
       return acquire(null);
    }

    public void requeueAll() throws InterruptedException {
        getStore().isolatedChange(new Callable<Object>() {
            @Override
            public Object call() throws InterruptedException {
                List<T> tasks = new LinkedList<T>();
                Iterator<T> running = getStore().getRunning();
                while(running.hasNext()) {
                    tasks.add(running.next());
                }

                for(T task : tasks) {
                    getStore().remove(task);
                    task.reset();
                    getStore().queue(task);

                    log.debug("Requeued task: " + task.getId());
                }
                return null;
            }
        });

    }

    private void acquireTask(T t) {
        t.setState(Task.State.RUNNING);
        t.setStarted(WatchProvider.currentTime());
        log.trace("Moving task to running pool");
        getStore().run(t);
        triggerAcquire(t);

        log.debug("Acquired task");
    }

    public T acquireTask(final UUID taskId) throws InterruptedException {
        return getStore().isolatedChange(new Callable<T>() {
            @Override
            public T call() throws Exception {
                T t = getStore().get(taskId);
                if (t == null) {
                    return null;
                }
                if (t.isRunning()) {
                    return null; //Already running
                }

                acquireTask(t);
                return t;
            }
        });

    }


}
