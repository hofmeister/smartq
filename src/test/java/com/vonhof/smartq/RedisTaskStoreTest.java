package com.vonhof.smartq;


import org.junit.Ignore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.UUID;

@Ignore
public class RedisTaskStoreTest extends TaskStoreTest {

    @Override
    protected RedisTaskStore<Task> makeStore() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxActive(1);

        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost",6379,0);
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis,Task.class);
        store.setNamespace(UUID.randomUUID().toString().replaceAll("(?uis)[^A-Z0-9]","")+"/");
        store.reset();
        return store;
    }
}
