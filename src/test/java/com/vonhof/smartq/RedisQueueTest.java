package com.vonhof.smartq;


import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RedisQueueTest extends SmartQTest {


    @Override
    protected TaskStore<Task> makeStore() {
        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost",6379,0);
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
        store.setNamespace(UUID.randomUUID().toString()+"/");

        return store;
    }

}
