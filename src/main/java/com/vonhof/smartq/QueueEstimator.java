package com.vonhof.smartq;


import org.apache.log4j.Logger;

import java.util.*;

public class QueueEstimator<T extends Task> {
    private final static Logger log = Logger.getLogger(QueueEstimator.class);

    private final SmartQ queue;
    private final TaskStore store;
    private List<Task> runningTasks = new LinkedList<Task>();
    private List<Task> onHold = new LinkedList<Task>();
    private List<Task> executionOrder = new LinkedList<Task>();
    private long time = 0;

    private FastCountMap runningTaskCount;
    private FastCountMap concurrencyCache;
    private FastCountMap estimates;

    public QueueEstimator(SmartQ queue) {
        this.queue = queue;
        this.store = queue.getStore();
    }

    public synchronized long queueEnds() throws InterruptedException {
        return taskStarts(null);
    }

    public long queueEnds(Iterator<T> queued) throws InterruptedException {
        return taskStarts(queued, null);
    }

    public long taskStarts(Task task) throws InterruptedException {
        return taskStarts(store.getPending(), task);
    }

    public synchronized long taskStarts(Iterator<T> queued, Task task) throws InterruptedException {
        runningTasks.clear();
        onHold.clear();
        executionOrder.clear();
        time = 0;
        int tagCount = store.getTags().size();
        concurrencyCache = new FastCountMap(tagCount);
        runningTaskCount = new FastCountMap(tagCount);
        estimates = new FastCountMap(tagCount);


        if (queue.getSubscribers() < 1) {
            throw new RuntimeException("Can not estimate queue with no subscribers");
        }

        while(queued.hasNext() || !onHold.isEmpty())  {
            time = markFirstDone(); //Moves time forward

            //Check if we had to skip some that now can be executed
            for(Task holding : new LinkedList<Task>(onHold)) {
                if (canRunNow(holding)) {
                    if (task != null &&
                            holding.getId().equals(task.getId())) {
                        return time;
                    }

                    if (onHold.remove(holding)) {
                        markAsRunning(holding);
                    }
                }
            }

            while(queued.hasNext()) {
                Task next = new Task(queued.next());
                next.setData(null); //No need to use mem on this

                //Pick from queue

                if (!canRunNow(next)) {
                    onHold.add(next);
                    if (!canRunAny()) {
                        break;
                    } else {
                        continue;
                    }
                }

                if (task != null &&
                        next.getId().equals(task.getId())) {
                    return time;
                }

                markAsRunning(next);
            }
        }

        while(!runningTasks.isEmpty()) {
            time = markFirstDone();
        }

        return time;
    }

    public synchronized List<Task> getLastExecutionOrder() {
        return Collections.unmodifiableList(executionOrder);
    }

    /**
     * Mark the fastest running tasks as done and returns the new simulated time
     */
    private long markFirstDone() {
        if (runningTasks.isEmpty()) {
            return time;
        }

        long fastest = time; //Now

        for(Task task : runningTasks) {

            long estimate = estimates.get(task.getType(), -1);
            if (estimate < 0) {
                estimate = estimates.set(task.getType(), queue.getEstimateForTaskType(task.getType()));
            }

            long estimatedEndTime = task.getStarted() + estimate;

            if (fastest == time || estimatedEndTime < fastest) {
                fastest = estimatedEndTime;
            }
        }

        for(Task task : new LinkedList<>(runningTasks)) {
            long estimatedEndTime = task.getStarted() + estimates.get(task.getType(),0);

            if (estimatedEndTime == fastest) {
                markAsDone(task, estimatedEndTime);
            }
        }

        return fastest;
    }



    private void markAsRunning(Task task) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Marking %s as running at %s", task, time));
        }

        runningTasks.add(task);
        if (task.isRunning()) {
            task.setStarted(time - (WatchProvider.currentTime() - task.getStarted()));
        } else {
            task.setStarted(time);
        }

        for(String tag: (Set<String>)task.getTagSet()) {
            runningTaskCount.increment(tag,1);
        }

        executionOrder.add(task);
    }

    private void markAsDone(Task task, long endTime) {
        if (!runningTasks.remove(task)) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Marking %s as done at %s", task, endTime));
        }

        task.setEnded(endTime);

        for(String tag: (Set<String>)task.getTagSet()) {
            runningTaskCount.decrement(tag, 1);
        }
    }

    private boolean canRunAny() throws InterruptedException {

        if (runningTasks.size() >= queue.getSubscribers() ||
                (queue.getConcurrency() > 0 && runningTasks.size() >= queue.getConcurrency())) {
            return false;
        }

        return true;
    }

    private boolean canRunNow(Task task) throws InterruptedException {

        if (!canRunAny()) {
            return false;
        }

        for(String tag: (Set<String>)task.getTagSet()) {
            long runningCount = runningTaskCount.get(tag, 0);
            long rateLimit = concurrencyCache.get(tag, -1);
            if (rateLimit < 0) {
                rateLimit = queue.getConcurrency(tag);
                concurrencyCache.set(tag, rateLimit);
            }
            if (rateLimit > 0 && runningCount >= rateLimit) {
                return false;
            }
        }

        return true;
    }



}
