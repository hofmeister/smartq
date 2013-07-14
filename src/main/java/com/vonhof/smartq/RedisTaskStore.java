package com.vonhof.smartq;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class RedisTaskStore<T extends Task> implements TaskStore<T> {
    private static final Logger log = Logger.getLogger(RedisTaskStore.class);

    private final ThreadLocal<String> LOCK_ID = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            return UUID.randomUUID().toString();
        }
    };

    private ObjectMapper om = new ObjectMapper();
    private final JedisPool jedisPool;

    private final Class<T> taskClass;

    private String queueList = "tasks/queued";
    private String runningList = "tasks/running";
    private String namespace = "smartq/";

    private final String lockMutux = "LOCK_MUTEX";

    public RedisTaskStore(JedisPool jedis,Class<T> taskClass) {
        this.jedisPool = jedis;
        this.taskClass = taskClass;
    }

    private String docId(UUID id) {
        return key("task/"+id.toString());
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    private String key(String key) {
        return namespace + key;
    }

    private T readTask(UUID id) {
        final Jedis jedis = jedisPool.getResource();
        try {
            String json = jedis.get(docId(id));
            if (json == null) {
                return null;
            }
            try {
                return om.readValue(json, taskClass);
            } catch (IOException e) {
                log.warn("Could not read task doc",e);
            }
            return null;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private List<T> readTasks(String listId) {
        final Jedis jedis = jedisPool.getResource();
        try {
            Set<String> ids = jedis.zrevrange(key(listId), 0, 200);
            List<T> tasks = new LinkedList<T>();
            if (ids.isEmpty()) {
                return tasks;
            }

            List<String> docs = jedis.mget(ids.toArray(new String[0]));

            for(String doc: docs) {
                if (doc == null) {
                    throw new NullPointerException("Task was null");
                }
                try {
                    T task = om.readValue(doc, taskClass);
                    tasks.add(task);
                } catch (IOException e) {
                    log.warn("Could not read json doc",e );
                }
            }
            return tasks;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private void clearList(String listId) {
        final Jedis jedis = jedisPool.getResource();
        try {
            Set<String> ids = jedis.zrevrange(key(listId), 0, -1);

            
            jedis.del(key(listId));

            if (!ids.isEmpty()) {
                jedis.del(ids.toArray(new String[0]));
            }
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private void writeTask(T task,Jedis jedis) {
        try {
            jedis.set(docId(task.getId()), om.writeValueAsString(task));
        } catch (IOException e) {
            log.warn("Could not write task doc",e);
        }
    }

    @Override
    public synchronized T get(UUID id) {
        return readTask(id);
    }

    @Override
    public void remove(T task) {
        remove(task.getId());
    }

    @Override
    public synchronized void remove(UUID id) {
        final Jedis jedis = jedisPool.getResource();
        try {
            
            jedis.del(docId(id));
            Long queueRemoved = jedis.zrem(key(queueList), docId(id));
            Long runRemoved = jedis.zrem(key(runningList), docId(id));

            jedis.decrBy(key("queued_count"), queueRemoved);
            jedis.decrBy(key("running_count"),runRemoved);
            
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized void queue(T task) {
        final Jedis jedis = jedisPool.getResource();
        try {
            
            writeTask(task, jedis);
            Long added = jedis.zadd(key(queueList), task.getPriority(), docId(task.getId()));

            jedis.incrBy(key("queued_count"), added);
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized void run(T task) {
        final Jedis jedis = jedisPool.getResource();
        try {
            
            writeTask(task, jedis);
            Long removed = jedis.zrem(key(queueList), docId(task.getId()));
            Long added = jedis.zadd(key(runningList), task.getPriority(), docId(task.getId()));

            jedis.decrBy(key("queued_count"), removed);
            jedis.incrBy(key("running_count"), added);
            
        } catch (RuntimeException e) {
            log.warn("Failed to move task to running pool",e);
            throw e;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized List<T> getQueued() {
        return readTasks(queueList);
    }

    @Override
    public synchronized List<T> getRunning() {
        return readTasks(runningList);
    }

    @Override
    public synchronized long queueSize() {
        final Jedis jedis = jedisPool.getResource();
        try {
            String count = jedis.get(key("queued_count"));
            if (count == null || count.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(count);
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized long runningCount() {
        final Jedis jedis = jedisPool.getResource();
        try {
            String count = jedis.get(key("running_count"));
            if (count == null || count.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(count);
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public void unlock() {
        final Jedis jedis = jedisPool.getResource();
        try {
            log.trace("Attempting to unlock");
            String lock = jedis.get(key("lock"));
            if (lock != null && lock.equalsIgnoreCase(LOCK_ID.get())) {
                jedis.del(key("lock"));
                jedis.publish(key("unlocked"),"unlocked");
                log.trace("Store unlocked");
            } else {
                log.trace("Failed to unlock store. Lock expired?");
            }
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public void lock() throws InterruptedException {
        while (true) {
            final Jedis jedis = jedisPool.getResource();
            try {
                Long locked = jedis.setnx(key("lock"), LOCK_ID.get());
                if (locked > 0) {
                    jedis.expire(key("lock"),30);
                    log.trace("Store locked by me");
                    return;
                }

                log.trace("Store already locked - waiting for unlock");
                jedis.subscribe(new LockSubscriber(), key("unlocked"));
                log.trace("Unlock was detected");
            } finally {
                log.trace("Returned lock listener resource to pool");
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public void waitForChange() throws InterruptedException {
        final Jedis jedis = jedisPool.getResource();
        try {
            log.trace("Waiting for changes");
            jedis.subscribe(new ChangeSubscriber(), key("changes"));
        } finally {
            log.trace("Returned change listener resource to pool");
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public void signalChange() {
        final Jedis jedis = jedisPool.getResource();
        try {
            log.trace("Signalling change");
            jedis.publish(key("changes"), "changed");
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    public synchronized void reset() {
        log.trace("Resetting store");
        
        clearList(queueList);
        clearList(runningList);

        final Jedis jedis = jedisPool.getResource();
        try {
            jedis.del(key("changes"));
            jedis.del(key("lock"));
            jedis.del(key("running_count"));
            jedis.del(key("queued_count"));
            
        } finally {
            jedisPool.returnResource(jedis);
        }

        log.trace("Store reset");
    }

    private class ChangeSubscriber extends MessageSubscriber {

        @Override
        public void onMessage(String channel, String message) {
            unsubscribe();

            synchronized (RedisTaskStore.this) {
                RedisTaskStore.this.notifyAll();
            }
        }
    }

    private class LockSubscriber extends MessageSubscriber {
        @Override
        public void onMessage(String channel, String message) {
            unsubscribe();

            synchronized (lockMutux) {
                lockMutux.notifyAll();
            }
        }

    }


    private abstract class MessageSubscriber extends JedisPubSub {
        @Override public void onPMessage(String pattern, String channel, String message) {}
        @Override public void onSubscribe(String channel, int subscribedChannels) {}
        @Override public void onUnsubscribe(String channel, int subscribedChannels) {}
        @Override public void onPUnsubscribe(String pattern, int subscribedChannels) {}
        @Override public void onPSubscribe(String pattern, int subscribedChannels) {}
    }
}
