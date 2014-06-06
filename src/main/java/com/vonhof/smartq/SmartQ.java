package com.vonhof.smartq;

import com.vonhof.smartq.Task.State;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class SmartQ<T extends Task,U>  {

    private static final Logger log = Logger.getLogger(SmartQ.class);

    private final Map<String, Integer> taskTagRateLimits = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Integer> taskTagRetryLimits = new ConcurrentHashMap<String, Integer>();

    private volatile int subscribers = 0;
    private final TaskStore<T> store;


    private final List<QueueListener> listeners = new ArrayList<QueueListener>();
    private boolean interrupted = false;
    private long defaultTaskEstimate = 60000;

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

    protected int getConcurrency(String tag) {
        int concurrency = getConcurrency();

        int concurrencyLimit = getRateLimit(tag);
        if (concurrencyLimit < 1) {
            return concurrency;
        } else {
            return Math.min(concurrencyLimit,concurrency);
        }
    }

    protected int getConcurrency(T task) {
        return getConcurrency(task.getTagSet());
    }

    protected int getConcurrency(Set<String> tags) {
        int concurrency = getConcurrency();
        for(String tag: tags) {
            int rateLimit = getRateLimit(tag);
            if (rateLimit > 0 && rateLimit < concurrency) {
                concurrency = rateLimit;
            }
        }
        return concurrency;
    }

    protected String getRateLimitingTag(T task) {
        return getRateLimitingTag(task.getTagSet());
    }

    protected String getRateLimitingTag(Set<String> tags) {
        int concurrency = getConcurrency();
        String out = null;
        for(String tag: tags) {
            int rateLimit = getRateLimit(tag);
            if ((rateLimit > 0 && rateLimit < concurrency) || tag == null) {
                concurrency = rateLimit;
                out = tag;
            }
        }

        return out;
    }

    /**
     * Limit the throughput of a specific tag ( e.g. how many tasks of the given tag that may be processed
     * concurrently )
     * @param tag
     * @param limit
     */
    public final void setRateLimit(String tag, int limit) {
        taskTagRateLimits.put(tag, limit);
    }

    /**
     * Gets the max allowed concurrent tasks for a given tag. Returns -1 if no limit is specified.
     * @param tag
     * @return
     */
    public final int getRateLimit(String tag) {
        if (taskTagRateLimits.containsKey(tag)) {
            return taskTagRateLimits.get(tag);
        }
        return -1;
    }

    public final void setMaxRetries(String tag, int limit) {
        taskTagRetryLimits.put(tag, limit);
    }

    public final int getMaxRetries(Set<String> tags) {
        int max = -1;
        for(String tag : tags) {
            if (taskTagRetryLimits.containsKey(tag) &&
                    (max == -1 || taskTagRetryLimits.get(tag) < max)) {
                max = taskTagRetryLimits.get(tag);
            }
        }

        return max;
    }

    /**
     * Get estimated time until queue is completely done
     * @param tag
     * @return
     */
    public long getEstimatedTimeLeft(String tag) throws InterruptedException {
        return new QueueEstimator(this).queueEnds(getStore().getPending(tag));
    }


    /**
     * Get estimated time until queue no longer is blocked
     * @return
     */
    public long getEstimatedTimeLeft() throws InterruptedException {
        return new QueueEstimator(this).queueEnds(getStore().getPending());
    }

    /**
     * Get estimated time until task can be executed
     * @return
     */
    public long getEstimatedStartTime(T task) throws InterruptedException {
        return new QueueEstimator(this).taskStarts(task);
    }

    public long getEstimateForTaskType(String type) {
        long average = getStore().getTaskTypeEstimate(type);
        if (average < 1) {
            average = defaultTaskEstimate;
        }
        return average;
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
        for(Map.Entry<String,Integer> entry : (Set<Map.Entry<String,Integer>>)task.getTags().entrySet()) {
            if (entry.getValue() > 0) {
                setRateLimit(entry.getKey(), entry.getValue());
            }
        }

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
                getStore().addTaskTypeDuration(task.getType(), task.getActualDuration());
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

                int maxRetries = getMaxRetries((Set<String>)task.getTagSet());
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
    
    public T acquire(final String tag) throws InterruptedException {

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

                            Iterator<T> queued = tag != null ? getStore().getQueued(tag) : getStore().getQueued();
                            T taskLookup = null;

                            lookupLoop:
                            while(queued.hasNext()) {
                                final T task = queued.next();

                                if (isRateLimited(tasksRunning, task)) {
                                    continue lookupLoop;
                                }

                                taskLookup = task;
                                break;
                            }

                            if (taskLookup != null) {
                                log.debug(String.format("Found task %s for tag %s", taskLookup.getId(), tag));
                                acquireTask(taskLookup);
                            } else {
                                log.debug(String.format("Found no tasks for tag %s", tag));
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

    public boolean isRateLimited(T task) throws InterruptedException {
        return isRateLimited(new CountMap<String>(), task);
    }

    private boolean isRateLimited(CountMap<String> tasksRunning, T task) throws InterruptedException {
        for(String tag : (Set<String>)task.getTagSet()) {
            int limit = getRateLimit(tag);

            if (limit > 0) {
                if (!tasksRunning.contains(tag)) {
                    tasksRunning.set(tag, getStore().runningCount(tag));
                }
                long running = tasksRunning.get(tag);

                if (running >= limit) {
                    log.debug(String.format("Rate limited tag: %s (Running: %s, Limit: %s)",tag, running, limit));
                    return true;
                }
            }
        }
        return false;
    }

    public int getRateLimit(T task) throws InterruptedException {
        int result = -1;
        for(String tag : (Set<String>)task.getTagSet()) {
            int limit = getRateLimit(tag);

            if (limit > 0 && (result == -1 || limit < result)) {
                result = limit;
            }
        }
        return result;
    }

    public List<T> getRunningTasks(String tag) {
        List<T> out = new LinkedList<T>();
        Iterator<T> running = getStore().getRunning(tag);
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

    public void setEstimateForTaskType(String type, long estimate) {
        getStore().setTaskTypeEstimate(type, estimate);
    }
}
