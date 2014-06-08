package com.vonhof.smartq;


import org.apache.log4j.Logger;

import java.util.*;

public class QueueEstimator<T extends Task> {
    private final static Logger log = Logger.getLogger(QueueEstimator.class);

    private long time = 0;
    private final SmartQ queue;
    private final TaskStore store;
    private List<TaskInfo> runningTasks = new LinkedList<>();
    private List<TaskInfo> onHold = new ArrayList<>(1000);
    private List<TaskInfo> executionOrder = new LinkedList<>();
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
        concurrencyCache = new FastCountMap(store.getTags(), -1);
        runningTaskCount = new FastCountMap(store.getTags(), 0);
        estimates = new FastCountMap(store.getTags(), -1);

        int maxHoldingSize = -1;

        if (queue.getSubscribers() < 1) {
            throw new RuntimeException("Can not estimate queue with no subscribers");
        }

        int holdingHits = 0;
        mainWhile:
        while(queued.hasNext() || !onHold.isEmpty())  {
            time = markFirstDone(); //Moves time forward

            //Check if we had to skip some that now can be executed
            int holdingSize = onHold.size();
            if (maxHoldingSize < holdingSize) {
                maxHoldingSize = holdingSize;
            }

            holdingHits++;
            for(TaskInfo holding : new ArrayList<>(onHold)) {
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

            if (holdingSize > 500) {
                continue; //Save some memory
            }

            while(queued.hasNext()) {
                TaskInfo next = new TaskInfo(queued.next());

                //Pick from queue

                if (!canRunNow(next)) {
                    onHold.add(next);
                    if (!canRunAny()) {
                        break;
                    } else {
                        if (onHold.size() > 500) {
                            continue mainWhile;//Save some memory
                        }
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

        //System.out.println("Max holding size was " + maxHoldingSize + " and hits on holding: " + holdingHits);

        while(!runningTasks.isEmpty()) {
            time = markFirstDone();
        }

        return time;
    }

    public synchronized List<TaskInfo> getLastExecutionOrder() {
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

        for(TaskInfo task : runningTasks) {

            long estimate = estimates.get(task.getType());
            if (estimate < 0) {
                estimate = estimates.set(task.getType(), queue.getEstimateForTaskType(task.getType()));
            }

            long estimatedEndTime = task.getStarted() + estimate;

            if (fastest == time || estimatedEndTime < fastest) {
                fastest = estimatedEndTime;
            }
        }

        for(TaskInfo task : new LinkedList<>(runningTasks)) {
            long estimatedEndTime = task.getStarted() + estimates.get(task.getType());

            if (estimatedEndTime == fastest) {
                markAsDone(task, estimatedEndTime);
            }
        }

        return fastest;
    }



    private void markAsRunning(TaskInfo task) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Marking %s as running at %s", task, time));
        }

        runningTasks.add(task);
        if (task.isRunning()) {
            task.setStarted(time - (WatchProvider.currentTime() - task.getStarted()));
        } else {
            task.setStarted(time);
        }

        for(String tag: task.getTags()) {
            runningTaskCount.increment(tag,1);
        }

        executionOrder.add(task);
    }

    private void markAsDone(TaskInfo task, long endTime) {
        if (!runningTasks.remove(task)) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Marking %s as done at %s", task, endTime));
        }

        task.setEnded(endTime);

        for(String tag: task.getTags()) {
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

    private boolean canRunNow(TaskInfo task) throws InterruptedException {

        if (!canRunAny()) {
            return false;
        }

        for(String tag: task.getTags()) {
            long runningCount = runningTaskCount.get(tag);
            long rateLimit = concurrencyCache.get(tag);
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
