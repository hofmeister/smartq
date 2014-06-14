package com.vonhof.smartq;

import com.vonhof.smartq.Task.State;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;

public class SmartQ<U>  {

    private static final Logger log = Logger.getLogger(SmartQ.class);



    private final TaskStore store;
    private volatile int subscribers = 0;
    private volatile int concurrency = -1;


    private final List<QueueListener> listeners = new ArrayList<QueueListener>();
    private boolean interrupted = false;
    private long defaultTaskEstimate = 60000;

    public SmartQ(final TaskStore store) {
        this.store = store;
    }

    public int getSubscribers() {
        return subscribers;
    }

    public TaskStore getStore() {
        return store;
    }

    public void setSubscribers(int subscribers) {
        this.subscribers = subscribers;
    }

    protected int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public void addListener(QueueListener listener) {
        listeners.add(listener);
    }

    private void triggerAcquire(Task task) {
        for(QueueListener listener : listeners) {
            listener.onAcquire(task);
        }
    }

    private void triggerSubmit(Task task) {
        for(QueueListener listener : listeners) {
            listener.onSubmit(task);
        }
    }

    private void triggerDone(Task task) {
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
            return concurrency > 0 ? Math.min(concurrencyLimit,concurrency) : concurrencyLimit;
        }
    }

    protected int getConcurrency(Task task) {
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

    protected String getRateLimitingTag(Task task) {
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
        getStore().setRateLimit(tag, limit);
    }

    /**
     * Gets the max allowed concurrent tasks for a given tag. Returns -1 if no limit is specified.
     * @param tag
     * @return
     */
    public final int getRateLimit(String tag) {
        int rateLimit = getStore().getRateLimit(tag);
        if (rateLimit > 0) {
            return rateLimit;
        }
        return getConcurrency();
    }

    public final void setMaxRetries(String tag, int limit) {
        getStore().setMaxRetries(tag, limit);
    }

    public final int getMaxRetries(Set<String> tags) {
        return getStore().getMaxRetries(tags);
    }



    public Map<String, Long> getEstimatesForReferenceGroups() throws InterruptedException {
        QueueEstimator estimator = new QueueEstimator(this);
        estimator.queueEnds(getStore().getPending());
        return estimator.getReferenceETA();
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
     * Gets the first task that has the given reference
     * @param referenceId
     * @return
     */
    public Task getFirstTaskWithReference(String referenceId) {
        return getStore().getFirstTaskWithReference(referenceId);
    }

    /**
     * Gets the last task that has the given reference
     * @param referenceId
     * @return
     */
    public Task getLastTaskWithReference(String referenceId) {
        return getStore().getLastTaskWithReference(referenceId);
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
    public long getEstimatedStartTime(Task task) throws InterruptedException {
        return new QueueEstimator(this).taskStarts(task);
    }

    /**
     * Get estimated time until tasks with references can be executed
     * @return
     */
    public long getEstimatedStartTime(String referenceId) throws InterruptedException {
        Task task = getFirstTaskWithReference(referenceId);
        if (task == null) {
            return 0;
        }
        return new QueueEstimator(this).taskStarts(task);
    }

    /**
     * Get estimated time until tasks with references are all done
     * @return
     */
    public long getEstimatedEndTime(String referenceId) throws InterruptedException {
        Task task = getLastTaskWithReference(referenceId);
        if (task == null) {
            return 0;
        }
        return new QueueEstimator(this).taskStarts(task) + getEstimateForTaskType(task.getType()); //include itself
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


    public boolean submit(Collection<Task> tasks) throws InterruptedException {
        return submit((Task[]) tasks.toArray(new Task[0]));
    }
    
    public boolean submit(final Task ... tasks) throws InterruptedException {
        Map<String,Integer> rateLimits = new HashMap<>();
        for(Task task : tasks) {
            for(Map.Entry<String,Integer> entry : (Set<Map.Entry<String,Integer>>)task.getTags().entrySet()) {
                if (entry.getValue() > 0 && !rateLimits.containsKey(entry.getKey())) {
                    rateLimits.put(entry.getKey(), entry.getValue());
                }
            }
        }

        for(Map.Entry<String,Integer> entry : rateLimits.entrySet()) {
            setRateLimit(entry.getKey(), entry.getValue());
        }

        getStore().queue(tasks);

        getStore().signalChange();

        for(Task task : tasks) {
            triggerSubmit(task);
        }

        return true;
    }

    public boolean cancel(UUID taskId) throws InterruptedException {
        return cancel(getStore().get(taskId));
    }

    public boolean cancel(Task task) throws InterruptedException {
        return cancel(task, false);
    }

    public boolean cancel(UUID taskId, final boolean reschedule) throws InterruptedException {
        Task t = getStore().get(taskId);
        return cancel(t, reschedule);
    }
    
    public boolean cancel(final Task task, final boolean reschedule) throws InterruptedException {
        if (task == null) {
            return false;
        }

        log.debug("Cancelled task");
        getStore().remove(task);
        task.setEnded(WatchProvider.currentTime());

        if (!reschedule) {
            getStore().signalChange();

        }

        if (reschedule) {
            task.reset();
            submit(task);
        } else {
            triggerDone(task);
        }

        return true;
    }

    public void cancelByReference(String referenceId) {
        getStore().cancelByReference(referenceId);
        getStore().signalChange();
    }

    
    public void acknowledge(final UUID id, U response) throws InterruptedException {
        final Task task = getStore().get(id);
        if (task == null) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Task not found: %s", id));
            }
            return;
        }

        task.setState(Task.State.DONE);
        task.setEnded(WatchProvider.currentTime());
        getStore().addTaskTypeDuration(task.getType(), task.getActualDuration());
        getStore().remove(task);
        getStore().signalChange();
        triggerDone(task);

        if (log.isDebugEnabled()) {
            log.debug("Acknowledged task: " + task.getId());
        }
    }

    public void failed(final UUID id) throws InterruptedException {

        final Task task = getStore().get(id);
        if (task == null) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Task not found: %s", id));
            }
            return;
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
    }

    public void interrupt() {
        interrupted = true;
        store.signalChange();
    }
    
    public Task acquire(final String tag) throws InterruptedException {

            interrupted = false;
            Task selectedTask = null;

            while(selectedTask == null) {

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Running tasks: %s", getStore().runningCount()));
                    log.debug(String.format("Queue queueSize: %s", getStore().queueSize()));
                }

                try {
                    selectedTask = getStore().isolatedChange(new Callable<Task>() {
                        @Override
                        public Task call() throws Exception {
                            long timeStart = System.currentTimeMillis();
                            CountMap<String> tasksRunning = new CountMap<String>();

                            final Iterator<UUID> queuedIds = tag != null ? getStore().getQueuedIds(tag) : getStore().getQueuedIds();

                            Task taskLookup = null;

                            lookupLoop:
                            while(queuedIds.hasNext()) {
                                final UUID taskId = queuedIds.next();
                                final Task task = getStore().get(taskId);

                                if (task == null || task.isRunning()) {
                                    if (log.isDebugEnabled()) {
                                        log.debug(String.format("Task not found: %s", taskId));
                                    }
                                    continue lookupLoop;
                                }

                                if (isRateLimited(tasksRunning, task)) {
                                    continue lookupLoop;
                                }

                                taskLookup = task;
                                break;
                            }

                            long timeTaken = System.currentTimeMillis() - timeStart;

                            if (taskLookup != null) {
                                if (log.isDebugEnabled()) {
                                    log.debug(String.format("Found task %s for tag %s in %s ms", taskLookup.getId(), tag, timeTaken));
                                }
                                acquireTask(taskLookup);
                            } else if (log.isDebugEnabled()) {
                                log.debug(String.format("Found no tasks for tag %s in %s ms", tag, timeTaken));
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

    public boolean isRateLimited(Task task) throws InterruptedException {
        return isRateLimited(new CountMap<String>(), task);
    }

    private boolean isRateLimited(CountMap<String> tasksRunning, Task task) throws InterruptedException {
        for(String tag : (Set<String>)task.getTagSet()) {
            int limit = getRateLimit(tag);

            if (limit > 0) {
                if (!tasksRunning.contains(tag)) {
                    tasksRunning.set(tag, getStore().runningCount(tag));
                }
                long running = tasksRunning.get(tag);

                if (running >= limit) {
                    if (log.isTraceEnabled()) {
                        log.trace(String.format("Rate limited tag: %s (Running: %s, Limit: %s)",tag, running, limit));
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public int getRateLimit(Task task) throws InterruptedException {
        int result = -1;
        for(String tag : (Set<String>)task.getTagSet()) {
            int limit = getRateLimit(tag);

            if (limit > 0 && (result == -1 || limit < result)) {
                result = limit;
            }
        }
        return result;
    }

    public List<Task> getRunningTasks(String tag) {
        List<Task> out = new LinkedList<Task>();
        Iterator<Task> running = getStore().getRunning(tag);
        while(running.hasNext()) {
            out.add(running.next());
        }

        return out;
    }


    public void acknowledge(UUID id) throws InterruptedException {
        acknowledge(id,null);
    }


    public Task acquire() throws InterruptedException {
       return acquire(null);
    }

    public void requeueAll() throws InterruptedException {
        getStore().isolatedChange(new Callable<Object>() {
            @Override
            public Object call() throws InterruptedException {
                List<Task> tasks = new LinkedList<Task>();
                Iterator<Task> running = getStore().getRunning();
                while (running.hasNext()) {
                    tasks.add(running.next());
                }

                for (Task task : tasks) {
                    getStore().remove(task);
                    task.reset();
                    getStore().queue(task);

                    if (log.isDebugEnabled()) {
                        log.debug("Requeued task: " + task.getId());
                    }

                }
                return null;
            }
        });

    }

    private void acquireTask(Task t) {
        t.setState(Task.State.RUNNING);
        t.setStarted(WatchProvider.currentTime());
        log.trace("Moving task to running pool");
        getStore().run(t);
        triggerAcquire(t);

        if (log.isDebugEnabled()) {
            log.debug("Acquired task: " + t.getId());
        }

    }

    public Task acquireTask(final UUID taskId) throws InterruptedException {
        return getStore().isolatedChange(new Callable<Task>() {
            @Override
            public Task call() throws Exception {
                Task t = getStore().get(taskId);
                if (t == null) {
                    return null;
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
