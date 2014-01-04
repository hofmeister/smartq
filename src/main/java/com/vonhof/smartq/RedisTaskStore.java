package com.vonhof.smartq;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public class RedisTaskStore<T extends Task> implements TaskStore<T> {

    private static final String TYPE_LIST = "tasks/types";

    private static final String RUNNING_COUNT = "running/count";
    private static final String RUNNING_LIST = "tasks/running";

    private static final String QUEUED_COUNT = "queued/count";
    private static final String QUEUED_ETA = "queued/eta";
    private static final String QUEUE_LIST = "tasks/queued";

    private static final String LOCK = "lock";
    private static final String CHANGES = "changes";


    private static final Logger log = Logger.getLogger(RedisTaskStore.class);


    private final ThreadLocal<String> LOCK_ID = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            return UUID.randomUUID().toString();
        }
    };


    private final JedisPool jedisPool;

    private final Class<T> taskClass;


    private String namespace = "smartq/";

    private DocumentSerializer documentSerializer = new JacksonDocumentSerializer();

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

    private String typedKey(String key, String type) {
        if (type == null) {
            return key(key);
        }
        return key(key + "/" + type);
    }

    public void setDocumentSerializer(DocumentSerializer documentSerializer) {
        this.documentSerializer = documentSerializer;
    }

    private T readTask(UUID id) {
        final Jedis jedis = jedisPool.getResource();
        try {
            String json = jedis.get(docId(id));
            if (json == null) {
                return null;
            }
            try {
                return documentSerializer.deserialize(json, taskClass);
            } catch (IOException e) {
                log.warn("Could not read task doc",e);
            }
            return null;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private Iterator<T> readTasks(String listId) {
        final Jedis jedis = jedisPool.getResource();
        try {
            return new SortedTaskIterator(key(listId));
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
                jedis.del(ids.toArray(new String[ids.size()]));
            }
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private void writeTask(T task,Jedis jedis) {
        try {
            jedis.set(docId(task.getId()), documentSerializer.serialize(task));
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

            final T task = get(id);
            if (task == null) {
                return;
            }

            jedis.del(docId(id));
            Long queueRemoved = jedis.zrem(key(QUEUE_LIST), docId(id));
            Long runRemoved = jedis.zrem(key(RUNNING_LIST), docId(id));

            if (task.getType() != null) {
                jedis.zrem(typedKey(RUNNING_LIST, task.getType()), docId(task.getId()));
                jedis.zrem(typedKey(QUEUE_LIST, task.getType()), docId(task.getId()));
            }

            decreaseQueued(jedis, task, queueRemoved);
            decreaseRunning(jedis, task, runRemoved);
            
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized void queue(T task) {
        final Jedis jedis = jedisPool.getResource();
        try {
            
            writeTask(task, jedis);
            Long added = jedis.zadd(key(QUEUE_LIST), task.getPriority(), docId(task.getId()));
            if (task.getType() != null) {
                jedis.zadd(typedKey(QUEUE_LIST, task.getType()), task.getPriority(), docId(task.getId()));
            }

            increaseQueued(jedis, task, added);

        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized void run(T task) {
        final Jedis jedis = jedisPool.getResource();
        try {
            
            writeTask(task, jedis);
            Long removed = jedis.zrem(key(QUEUE_LIST), docId(task.getId()));
            Long added = jedis.zadd(key(RUNNING_LIST), task.getPriority(), docId(task.getId()));

            if (task.getType() != null) {
                jedis.zrem(typedKey(QUEUE_LIST, task.getType()), docId(task.getId()));
                jedis.zadd(typedKey(RUNNING_LIST, task.getType()), task.getPriority(), docId(task.getId()));
            }

            decreaseQueued(jedis, task, removed);

            increaseRunning(jedis, task, added);
            
        } catch (RuntimeException e) {
            log.warn("Failed to move task to running pool",e);
            throw e;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized Iterator<T> getQueued() {
        return readTasks(QUEUE_LIST);
    }

    @Override
    public synchronized Iterator<T> getQueued(String type) {
        return readTasks(QUEUE_LIST + "/" + type);
    }

    @Override
    public synchronized Iterator<T> getRunning() {
        return readTasks(RUNNING_LIST);
    }

    @Override
    public Iterator<T> getRunning(String type) {
        return readTasks(RUNNING_LIST + "/" + type);
    }

    @Override
    public synchronized long queueSize() {
        final Jedis jedis = jedisPool.getResource();
        try {
            String count = jedis.get(key(QUEUED_COUNT));
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
        return runningCount(null);
    }

    @Override
    public synchronized long queueSize(String type) {
        final Jedis jedis = jedisPool.getResource();
        try {

            String count = jedis.get(typedKey(QUEUED_COUNT,type));

            if (count == null || count.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(count);
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized long runningCount(String type) {
        final Jedis jedis = jedisPool.getResource();
        try {
            String count = jedis.get(typedKey(RUNNING_COUNT,type));

            if (count == null || count.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(count);
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized Set<String> getTypes() {
        final Jedis jedis = jedisPool.getResource();
        try {
            return jedis.smembers(key(TYPE_LIST));
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized long getQueuedETA(String type) {
        final Jedis jedis = jedisPool.getResource();
        try {
            return Long.valueOf(jedis.get(typedKey(QUEUED_ETA, type)));
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public synchronized long getQueuedETA() {
        return getQueuedETA(null);
    }


    public void unlock() {
        final Jedis jedis = jedisPool.getResource();
        try {
            log.trace("Attempting to unlock");
            String lock = jedis.get(key(LOCK));
            if (lock != null && lock.equalsIgnoreCase(LOCK_ID.get())) {
                jedis.del(key(LOCK));
                jedis.publish(key("unlocked"),"unlocked");
                log.trace("Store unlocked");
            } else {
                log.info("Failed to unlock store. Lock expired?");
            }
        } finally {
            jedisPool.returnResource(jedis);
        }
    }


    public void lock() {
        while (true) {
            final Jedis jedis = jedisPool.getResource();
            try {
                Long locked = jedis.setnx(key(LOCK), LOCK_ID.get());
                if (locked > 0) {
                    jedis.expire(key(LOCK),30);
                    log.trace("Store locked by me");
                    jedisPool.returnResource(jedis);
                    return;
                }

                log.trace("Store already locked - waiting for unlock");
                jedis.subscribe(new LockSubscriber(), key("unlocked"));
                log.trace("Unlock was detected");
                jedisPool.returnResource(jedis);
            } catch (Exception ex) {
                log.error("Got exception while waiting for lock", ex);
                jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    @Override
    public synchronized <U> U isolatedChange(Callable<U> callable) throws InterruptedException {
        lock();
        try {
            return callable.call();
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            unlock();
        }
    }

    @Override
    public void waitForChange() throws InterruptedException {
        final Jedis jedis = jedisPool.getResource();
        try {
            log.trace("Waiting for changes");
            jedis.subscribe(new ChangeSubscriber(), key(CHANGES));
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
            jedis.publish(key(CHANGES), "changed");
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private void increaseRunning(Jedis jedis, T task, long count) {
        jedis.incrBy(key(RUNNING_COUNT), count);
        if (task.getType() != null) {
            jedis.incrBy(typedKey(RUNNING_COUNT,task.getType()), count);
        }
    }

    private void decreaseRunning(Jedis jedis, T task, long count) {
        if (count < 1) return;

        jedis.decrBy(key(RUNNING_COUNT), count);
        if (task.getType() != null) {
            jedis.decrBy(typedKey(RUNNING_COUNT,task.getType()), count);
        }
    }

    private void increaseQueued(Jedis jedis, T task, long count) {
        jedis.incrBy(key(QUEUED_COUNT), count);
        if (task.getType() != null) {
            jedis.incrBy(typedKey(QUEUED_COUNT,task.getType()), count);
        }

        jedis.incrBy(key(QUEUED_ETA), task.getEstimatedDuration());
        if (task.getType() != null) {
            jedis.incrBy(typedKey(QUEUED_ETA,task.getType()), task.getEstimatedDuration());
        }

        ensureType(jedis, task.getType());
    }

    private void decreaseQueued(Jedis jedis, T task, long count) {
        if (count < 1) return;

        jedis.decrBy(key(QUEUED_COUNT), count);
        if (task.getType() != null) {
            jedis.decrBy(typedKey(QUEUED_COUNT,task.getType()), count);
        }

        jedis.decrBy(key(QUEUED_ETA), task.getEstimatedDuration());
        if (task.getType() != null) {
            jedis.decrBy(typedKey(QUEUED_ETA, task.getType()), task.getEstimatedDuration());
        }
    }

    private void ensureType(Jedis jedis, String type) {
        if (type == null) return;
        jedis.sadd(key(TYPE_LIST),type);
    }

    public synchronized void reset() {
        log.trace("Resetting store");
        
        clearList(QUEUE_LIST);
        clearList(RUNNING_LIST);

        final Jedis jedis = jedisPool.getResource();
        try {
            Pipeline pipe = jedis.pipelined();
            pipe.del(
                key(CHANGES),
                key(LOCK),
                key(RUNNING_COUNT),
                key(QUEUED_COUNT),
                key(QUEUED_ETA)
            );

            for(String type : getTypes()) {
                pipe.del(
                    typedKey(RUNNING_COUNT, type),
                    typedKey(QUEUED_COUNT, type),
                    typedKey(RUNNING_LIST, type),
                    typedKey(QUEUE_LIST, type),
                    typedKey(QUEUED_ETA, type)
                );
            }
            pipe.sync();

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
        }

    }

    private class SortedTaskIterator implements Iterator<T> {

        private final String key;
        private int offset = 0;
        private T next = null;

        private SortedTaskIterator(String key) {
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            Jedis jedis = jedisPool.getResource();
            try {
                while(true) {
                    Set<String> result = jedis.zrevrange(key, offset, offset);
                    offset++;
                    if (result.isEmpty()) {
                        return false;
                    }

                    String id = result.iterator().next();
                    String doc = jedis.get(id);

                    if (doc == null) {
                        throw new NullPointerException("Task was null");
                    }

                    try {
                        next = documentSerializer.deserialize(doc, taskClass);
                    } catch (IOException e) {
                        log.warn("Could not read json doc",e );
                        continue;
                    }

                    return true;
                }
            } finally {
                jedisPool.returnResource(jedis);
            }
        }

        @Override
        public T next() {
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can not remove from task iterator");
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
