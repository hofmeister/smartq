package com.vonhof.smartq;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class QueueEstimator {
    private final static Logger log = Logger.getLogger(QueueEstimator.class);
    private final static ExecutorService pool;

    static {
        pool = Executors.newFixedThreadPool(20, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "smartq-estimator");
            }
        });
    }

    private long time = 0;
    private final SmartQ queue;
    private final TaskStore store;
    private final List<TaskInfo> runningTasks = new LinkedList<>();
    private final List<TaskInfo> onHold = new ArrayList<>(1000);
    private final List<TaskInfo> executionOrder = new LinkedList<>();
    private final HashMap<String, Long> referenceMap = new HashMap<>();
    private FastCountMap runningTaskCount;
    private FastCountMap concurrencyCache;
    private FastCountMap estimates;
    private Speed speed = Speed.AUTO;

    public QueueEstimator(SmartQ queue) {
        this.queue = queue;
        this.store = queue.getStore();
    }

    public void setSpeed(Speed speed) {
        this.speed = speed;
    }

    public synchronized long queueEnds() throws InterruptedException {
        return taskStarts(null);
    }

    public long queueEnds(ParallelIterator<Task> queued) throws InterruptedException {
        return taskStarts(queued, null);
    }

    public long taskStarts(Task task) throws InterruptedException {
        return taskStarts(store.getPending(), task);
    }

    public synchronized long taskStarts(ParallelIterator<Task> queued, final Task task) throws InterruptedException {

        if (speed.equals(Speed.AUTO) && queued.size() > 0) {
            if (queued.size() > 500000) {
                speed = Speed.FASTEST;
            } else if (queued.size() > 100000) {
                speed = Speed.FAST;
            } else {
                speed = Speed.EXACT;
            }
        }

        boolean doInParallel = speed.getSpeed() >= Speed.FAST.getSpeed();
        if (doInParallel &&
                queued.canDoParallel() &&
                task == null) {

            final ParallelIterator[] its = queued.getParallelIterators();
            if (its.length > 1) {
                final List<Future<Long>> futures = new LinkedList<>();

                for(final ParallelIterator it : its) {
                    futures.add(pool.submit(new Callable<Long>() {
                        @Override
                        public Long call() throws Exception {
                            final QueueEstimator estimator = new QueueEstimator(queue);
                            estimator.speed = speed;
                            return estimator.calculateETA(it, task);
                        }
                    }));
                }

                long out = 0;
                for(Future<Long> f : futures) {
                    try {
                        out += f.get();
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                return out;
            }
        }

        return calculateETA(queued, task);
    }

    public synchronized List<TaskInfo> getLastExecutionOrder() {
        return Collections.unmodifiableList(executionOrder);
    }



    public Map<String, Long> getReferenceETA() {
        return referenceMap;
    }

    private long calculateETA(ParallelIterator<Task> queued, Task task) throws InterruptedException {
        try {

            runningTasks.clear();
            onHold.clear();
            executionOrder.clear();
            referenceMap.clear();
            time = 0;
            concurrencyCache = new FastCountMap(store.getTags(), -1);
            runningTaskCount = new FastCountMap(store.getTags(), 0);
            estimates = new FastCountMap(store.getTags(), -1);

            int maxHoldingSize = -1;

            if (queue.getSubscribers() < 1) {
                throw new RuntimeException("Can not estimate queue with no subscribers");
            }

            boolean doGuesstimate = speed.getSpeed() >= Speed.FASTEST.getSpeed();
            int taskCount = 0;
            int holdingHits = 0;
            long size = queued.size();
            long criticalMass = Math.max(15000, size / 5L);
            mainWhile:
            while(queued.hasNext() || !onHold.isEmpty())  {
                time = markFirstDone(); //Moves time forward

                //Check if we had to skip some that now can be executed
                int holdingSize = onHold.size();
                if (maxHoldingSize < holdingSize) {
                    maxHoldingSize = holdingSize;
                }


                if (doGuesstimate && taskCount > criticalMass) {

                    double taskAvg = (double)time / (double)taskCount;
                    return (long) Math.ceil((double)size * taskAvg);
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
                            taskCount++;
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
                    taskCount++;
                }
            }

            //System.out.println("Max holding size was " + maxHoldingSize + " and hits on holding: " + holdingHits);

            while(!runningTasks.isEmpty()) {
                time = markFirstDone();
            }

            referenceMap.put("total",time);
            return time;
        } finally {
            queued.close();
        }
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
        if (log.isTraceEnabled()) {
            log.trace(String.format("Marking %s as running at %s", task, time));
        }

        runningTasks.add(task);
        if (task.isRunning()) {
            task.setStarted(time - (WatchProvider.currentTime() - task.getStarted()));
        } else {
            task.setStarted(time);
        }

        for(String tag: task.getTags()) {
            runningTaskCount.increment(tag, 1);
        }

        if (executionOrder.size() < 300) {
            executionOrder.add(task);
        }
    }

    private void markAsDone(TaskInfo task, long endTime) {
        if (!runningTasks.remove(task)) {
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace(String.format("Marking %s as done at %s", task, endTime));
        }

        task.setEnded(endTime);
        if (!StringUtils.isEmpty(task.getReferenceId())) {
            referenceMap.put(task.getReferenceId(), endTime);
        }

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


    public static enum Speed {
        /**
         * Gives the exact ETA of the queued tasks. This is the default speed.
         */
        EXACT(1),
        /**
         * Gives a near-exact ETA of the queued tasks. Runs bigger queues in parallel thus losing some
         * level of accuracy. The bigger the queue bigger the effect this has on the speed. Use
         * with queues of (60K+)
         */
        FAST(2),
        /**
         * Gives a rough guesstimate of the ETA by only calculating some of the queues and doing it in parallel.
         * Only use this on very large queues (120K+)
         */
        FASTEST(2),
        /**
         * Chooses the best speed for the given queue. Note that this might cause loss of accuracy.
         */
        AUTO(-1);

        private int speed;

        private Speed(int speed) {
            this.speed = speed;
        }

        public int getSpeed() {
            return speed;
        }
    }


}
