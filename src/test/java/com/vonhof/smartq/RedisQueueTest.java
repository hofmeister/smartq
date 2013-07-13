package com.vonhof.smartq;


import redis.clients.jedis.Jedis;

import java.util.UUID;

public class RedisQueueTest extends SmartQTest {


    @Override
    protected TaskStore<Task> makeStore() {
        Jedis jedis = new Jedis("localhost");
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
        store.setNamespace(UUID.randomUUID().toString()+"/");
        store.reset();

        return store;
    }
}
