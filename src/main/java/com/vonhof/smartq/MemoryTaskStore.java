package com.vonhof.smartq;

import com.vonhof.smartq.Task.State;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class MemoryTaskStore<T extends Task> implements TaskStore<T> {
    private static final Logger log = Logger.getLogger(MemoryTaskStore.class);
    private final Map<UUID, T> tasks = new ConcurrentHashMap<UUID, T>();
    private final List<T> queuedTasks = Collections.synchronizedList(new LinkedList<T>());
    private final List<T> runningTasks = Collections.synchronizedList(new LinkedList<T>());
    private final List<T> failedTasks = Collections.synchronizedList(new LinkedList<T>());
    private final CountMap<String> runningTypeCount = new CountMap<String>();
    private final CountMap<String> queuedTypeCount = new CountMap<String>();
    private EstimateMap<String> typeEstimate = new EstimateMap<String>();

    private final Lock lock = new ReentrantLock();

    private ThreadLocal<UUID> localTID = new ThreadLocal<UUID>() {
        @Override
        protected UUID initialValue() {
            return UUID.randomUUID();
        }
    };

    private volatile UUID tid = null;


    @Override
    public synchronized T get(UUID id) {
        return tasks.get(id);
    }

    @Override
    public synchronized void remove(T task) {
        tasks.remove(task.getId());
        queuedTasks.remove(task);
        runningTasks.remove(task);
        for(String tag : (Set<String>)task.getTagSet()) {
            runningTypeCount.decrement(tag, 1);
            queuedTypeCount.decrement(tag,1);    
        }
        
    }

    @Override
    public synchronized void remove(UUID id) {
        remove(get(id));
    }

    @Override
    public synchronized void queue(T task) {
        task.setState(State.PENDING);
        tasks.put(task.getId(), task);

        queuedTasks.add(task);
        for(String tag : (Set<String>)task.getTagSet()) {
            queuedTypeCount.increment(tag,1);
        }

        sort(queuedTasks);
    }

    @Override
    public synchronized void run(T task) {
        task.setState(State.RUNNING);
        queuedTasks.remove(task);
        runningTasks.add(task);

        for(String tag : (Set<String>)task.getTagSet()) {
            queuedTypeCount.decrement(tag, 1);
            runningTypeCount.increment(tag,1);
        }


        sort(runningTasks);
    }

    @Override
    public synchronized void failed(T task) {
        task.setState(State.ERROR);
        remove(task);
        tasks.put(task.getId(),task);
        failedTasks.add(task);
    }

    @Override
    public synchronized Iterator<T> getFailed() {
        return Collections.unmodifiableList(new LinkedList<T>(failedTasks)).iterator();
    }

    @Override
    public synchronized Iterator<T> getQueued() {
        return Collections.unmodifiableList(new LinkedList<T>(queuedTasks)).iterator();
    }

    @Override
    public synchronized Iterator<T> getPending() {
        return new CombinedIterator<T>(getRunning(), getQueued());
    }

    @Override
    public synchronized Iterator<T> getPending(String tag) {
        return new CombinedIterator(getRunning(tag), getQueued(tag));
    }

    @Override
    public long getTaskTypeEstimate(String type) {
        return typeEstimate.average(type);
    }

    @Override
    public void addTaskTypeDuration(String type, long duration) {
        typeEstimate.add(type, duration);
    }

    @Override
    public void setTaskTypeEstimate(String type, long estimate) {
        typeEstimate.set(type, estimate);
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Iterator<T> getQueued(String type) {
        List<T> out = new LinkedList<T>();
        for(T task : queuedTasks) {
            if (task.getTags().containsKey(type)) {
                out.add(task);
            }
        }

        sort(out);
        return Collections.unmodifiableList(out).iterator();
    }

    @Override
    public Iterator<UUID> getQueuedIds() {
        List<UUID> out = new LinkedList<UUID>();
        Iterator<T> queued = getQueued();
        while(queued.hasNext()) {
            out.add(queued.next().getId());
        }
        return out.iterator();
    }

    @Override
    public Iterator<UUID> getQueuedIds(String type) {
        List<UUID> out = new LinkedList<UUID>();
        Iterator<T> queued = getQueued(type);
        while(queued.hasNext()) {
            out.add(queued.next().getId());
        }
        return out.iterator();
    }

    public synchronized Iterator<T> getRunning() {
        return Collections.unmodifiableList(new LinkedList<T>(runningTasks)).iterator();
    }

    @Override
    public Iterator<T> getRunning(String type) {
        List<T> out = new LinkedList<T>();
        for(T task : runningTasks) {
            if (task.getTags().containsKey(type)) {
                out.add(task);
            }
        }

        sort(out);
        return Collections.unmodifiableList(out).iterator();
    }

    @Override
    public synchronized long queueSize() {
        return queuedTasks.size();
    }

    @Override
    public synchronized long runningCount() {
        return runningTasks.size();
    }

    @Override
    public long queueSize(String type) {
        return queuedTypeCount.get(type);
    }

    @Override
    public long runningCount(String type) {
        return runningTypeCount.get(type);
    }

    @Override
    public Set<String> getTags() {
        return Collections.unmodifiableSet(queuedTypeCount.keySet());
    }

    @Override
    public  <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
        UUID lTid = localTID.get();
        if (!lTid.equals(tid)) {
            log.debug("Locking TID: "+lTid);
            lock.lock();
            tid = lTid;
            log.debug("Locked TID: "+lTid);
        } else {
            log.debug("Within TID: "+lTid);
        }
        try {
            return callable.call();
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (lTid.equals(tid)) {
                tid = null;
                log.debug("Unlocking TID: "+lTid);
                lock.unlock();
            }
        }
    }

    @Override
    public synchronized void waitForChange() throws InterruptedException {
        log.debug("Waiting for change");
        this.wait();
    }

    @Override
    public synchronized void signalChange() {
        log.debug("Signalling change");
        this.notifyAll();
    }


    private void sort(List<T> tasks) {
        Collections.sort(tasks,new Comparator<T>() {
            @Override
            public int compare(T t, T t2) {
                int diffPrio = t2.getPriority()-t.getPriority();
                if (diffPrio == 0) {
                    return (int) ( (t.getCreated() / 1000L) - (t2.getCreated() / 1000L) );
                }
                return diffPrio;
            }
        });
    }

}
