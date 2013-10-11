package com.vonhof.smartq.example;


import com.vonhof.smartq.DefaultTaskResult;
import com.vonhof.smartq.RedisTaskStore;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.server.SmartQProducer;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;

public class DemoProducer {
    public static final InetSocketAddress ADDRESS = new InetSocketAddress("127.0.0.1",54321);

    public static void main(String[] args) throws IOException, InterruptedException {
        PropertyConfigurator.configure("log4j.properties");

        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost",6379,0);
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
        store.setNamespace("demo/");

        final SmartQ<Task, DefaultTaskResult> queue = new SmartQ<Task, DefaultTaskResult>(store);

        queue.requeueAll(); //If any was left as running - move them back to queue.

        final SmartQProducer<Task> producer = new SmartQProducer<Task>(ADDRESS, queue);

        producer.listen();

        Thread emitter = new Thread() {
            @Override
            public void run() {
                while(!interrupted()) {
                    try {
                        Task task = new Task("test");
                        System.out.println(new Date() + " - Task submitted: " + task.getId());
                        queue.submit(task);
                        Thread.sleep(5000 + (int)(50000*Math.random()));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        };

        emitter.start();
        emitter.join();
    }
}
