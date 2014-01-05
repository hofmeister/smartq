package com.vonhof.smartq.server;


import com.vonhof.smartq.RedisTaskStore;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.TaskStore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.UUID;

public class RedisClientServerTest extends ClientServerTest {

    @Override
    protected TaskStore<Task> makeStore() {
        final JedisPool jedis = new JedisPool(new JedisPoolConfig(),"localhost",6379,0);
        RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);
        store.setNamespace(UUID.randomUUID().toString()+"/");

        return store;
    }
}
