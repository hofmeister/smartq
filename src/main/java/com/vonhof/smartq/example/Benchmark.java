package com.vonhof.smartq.example;


import com.vonhof.smartq.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {
    public static final MemoryTaskStore STORE = new MemoryTaskStore();
    public static BenchmarkListener benchmarkListener;

    private static TaskStore makePGStore() throws SQLException, IOException {
        PostgresTaskStore store = new PostgresTaskStore(Task.class);
        store.setTableName("benchmark_queue");
        store.createTable();
        store.reset();

        return store;
    }

    private static TaskStore makeMemStore() {
        return STORE;
    }

    private static TaskStore makeStore() throws SQLException, IOException {
        return makePGStore();
    }

    public static void main (String[] args) throws Exception {

        final SmartQ<DefaultTaskResult> queue = new SmartQ<DefaultTaskResult>(makeStore());


        benchmarkListener = new BenchmarkListener(queue);
        queue.addListener(benchmarkListener);

        List<StressSubscriber> subscribers = new ArrayList<StressSubscriber>();

        for(int i = 0; i < 10; i++) {
            StressSubscriber subscriber = new StressSubscriber(i);
            subscribers.add(subscriber);
            subscriber.start();
        }

        System.out.println("Submitting tasks");


        for(int i = 0; i < 10; i++) {
            StressPublisher publisher = new StressPublisher(i);
            publisher.start();
        }

        System.out.println("Done submitting");


        while(true) {
            Thread.sleep(1000);

            if (queue.size() < 1) {
                break;
            }
        }

        System.out.println("Queue is done");

        for(StressSubscriber subscriber: subscribers) {
            subscriber.interrupt();
        }

        System.out.println("All done");

        benchmarkListener.close();
        queue.getStore().close();
    }

    private static class BenchmarkListener implements QueueListener {

        private final AtomicInteger acquires = new AtomicInteger(0);
        private final AtomicInteger submits = new AtomicInteger(0);
        private final AtomicInteger dones = new AtomicInteger(0);
        private long interval = 1000;
        private Timer timer = new Timer();
        private long first;
        private final SmartQ<DefaultTaskResult> queue;

        private BenchmarkListener(final SmartQ<DefaultTaskResult> queue) {
            this.queue = queue;

            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (first < 1) {
                         return;
                    }

                    int secs = (int) ((System.currentTimeMillis()-first) / 1000);

                    if (secs < 1) return;

                    int aSince = acquires.getAndSet(0);
                    int sSince = submits.getAndSet(0);
                    int dSince = dones.getAndSet(0);

                    int aPerSec = aSince / secs;
                    int sPerSec = sSince / secs;
                    int dPerSec = dSince / secs;

                    System.out.println(String.format("Acquires: %s (%s / s) | Submits: %s (%s / s) | Done: %s (%s / s) | Time: %s secs",
                            aSince,aPerSec,
                            sSince,sPerSec,
                            dSince,dPerSec,
                            secs));
                }
            }, interval, interval);

        }

        @Override
        public void onAcquire(Task t) {
            if (first == 0) {
                first = System.currentTimeMillis();
            }
            acquires.incrementAndGet();
        }

        @Override
        public void onSubmit(Task t) {
            submits.incrementAndGet();
        }

        @Override
        public void onDone(Task t) {
            dones.incrementAndGet();
        }

        public void close() {
            timer.cancel();

            try {
                queue.getStore().close();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private static class StressPublisher extends Thread {
        private static final Logger log = Logger.getLogger(StressPublisher.class);
        private final SmartQ<DefaultTaskResult> queue;

        private StressPublisher(int num) throws IOException, SQLException {
            super("Stress Publisher "+num);

            queue = new SmartQ<DefaultTaskResult>(makeStore());
            queue.addListener(benchmarkListener);
        }

        @Override
        public void run() {
            List<Task> tasks = new LinkedList<Task>();
            for(int i = 0; i < 10000; i++) {
                tasks.add(new Task("test")
                        .withPriority((int) Math.round(Math.random() * 10)));
            }

            log.info("Submitting " + tasks.size() + " tasks");

            try {
                queue.submit(tasks);

                log.info("Submitting " + tasks.size() + " tasks - done");
            } catch (InterruptedException e) {
                return;
            }

            try {
                queue.getStore().close();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private static class StressSubscriber extends Thread {
        private static final Logger log = Logger.getLogger(StressSubscriber.class);
        private final SmartQ<DefaultTaskResult> queue;

        private StressSubscriber(int num) throws IOException, SQLException {
            super("Stress subscriber "+num);

            queue = new SmartQ<DefaultTaskResult>(makeStore());
            queue.addListener(benchmarkListener);
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Task t = queue.acquire();

                    /*
                    log.info("Performing work");
                    synchronized (this) {
                        wait((int) Math.round(Math.random() * 10000));
                    }

                    log.info("Work done");
                    */

                    queue.acknowledge(t.getId());
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    log.error("Failed to ack", e);
                }
            }

            log.info("Subscriber closing");

            try {
                queue.getStore().close();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}
