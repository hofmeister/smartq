package com.vonhof.smartq;


import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RedisTaskStoreTest {

    private RedisTaskStore<Task> makeStore() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxActive(1);

        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost",6379,0);
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis,Task.class);;
        store.setNamespace(UUID.randomUUID().toString().replaceAll("(?uis)[^A-Z0-9]","")+"/");
        store.reset();
        return store;
    }

    @Test
    public void can_add_and_remove() {
        RedisTaskStore<Task> store = makeStore();

        assertEquals("SmartQ is empty",0,store.queueSize());
        assertEquals("Running is empty",0,store.runningCount());


        Task t = new Task("test");
        store.queue(t);

        assertEquals("Task is stored",1,store.queueSize());

        assertNotNull("Task can be fetched",store.get(t.getId()));

        store.run(t);

        assertEquals("Task is moved to running list",0,store.queueSize());
        assertEquals("Task is moved to running list",1,store.runningCount());

        assertNotNull("Task can be fetched",store.get(t.getId()));

        store.remove(t);

        assertEquals("Task can be removed",0,store.queueSize());
        assertEquals("Task can be removed",0,store.runningCount());

        assertNull("Task can not be fetched", store.get(t.getId()));
    }



}
