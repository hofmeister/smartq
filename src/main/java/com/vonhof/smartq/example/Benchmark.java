package com.vonhof.smartq.example;


import com.vonhof.smartq.DefaultTaskResult;
import com.vonhof.smartq.MemoryTaskStore;
import com.vonhof.smartq.QueueListener;
import com.vonhof.smartq.RedisTaskStore;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {
    public static final BenchmarkListener BENCHMARK_LISTENER = new BenchmarkListener();
    public static final MemoryTaskStore<Task> STORE = new MemoryTaskStore<Task>();

    public static void main (String[] args) throws InterruptedException {
        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost");
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
        store.setNamespace("benchmark");
        store.reset();
        final SmartQ<Task, DefaultTaskResult> queue = new SmartQ<Task, DefaultTaskResult>(store);
        queue.addListener(BENCHMARK_LISTENER);

        List<StressSubscriber> subscribers = new ArrayList<StressSubscriber>();

        for(int i = 0; i < 16; i++) {
            StressSubscriber subscriber = new StressSubscriber(i);
            subscribers.add(subscriber);
            subscriber.start();
        }


        for(int i = 0; i < 100000; i++) {
            queue.submit(new Task()
                    .withPriority((int) Math.round(Math.random() * 10)));
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

        BENCHMARK_LISTENER.close();
    }

    private static class BenchmarkListener implements QueueListener {

        private final AtomicInteger acquires = new AtomicInteger(0);
        private final AtomicInteger submits = new AtomicInteger(0);
        private final AtomicInteger dones = new AtomicInteger(0);
        private long interval = 1000;
        private Timer timer = new Timer();
        private long first;

        private BenchmarkListener() {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    int aSince = acquires.intValue();
                    int sSince = submits.intValue();
                    int dSince = dones.intValue();

                    if (first < 1) {
                         return;
                    }
                    int secs = (int) ((System.currentTimeMillis()-first) / 1000);

                    if (secs < 1) return;

                    int aPerSec = aSince / secs;
                    int sPerSec = sSince / secs;
                    int dPerSec = dSince / secs;

                    System.out.println(String.format("Acquires: %s (%s / s) | Submits: %s (%s / s) | Done: %s (%s / s) | Time: %s",
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
        }
    }

    private static class StressSubscriber extends Thread {
        private final SmartQ<Task, DefaultTaskResult> queue;

        private StressSubscriber(int num) {
            super("Stress subscriber "+num);
            final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost");
            final RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
            store.setNamespace("benchmark");
            queue = new SmartQ<Task, DefaultTaskResult>(store);
            queue.addListener(BENCHMARK_LISTENER);
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Task t = queue.acquire();
                    queue.acknowledge(t.getId());
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
