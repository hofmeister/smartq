package com.vonhof.smartq;


import redis.clients.jedis.Jedis;

import java.util.UUID;

public class RedisQueueTest extends DefaultQueueTest {

    @Override
    protected Queue<Task, DefaultTaskResult> makeQueue() {
        Jedis jedis = new Jedis("localhost");
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis,Task.class);
        store.setNamespace(UUID.randomUUID().toString().replaceAll("(?uis)[^A-Z0-9]","")+"/");
        store.reset();

        return new DefaultQueue<Task, DefaultTaskResult>(store);
    }
}
