package com.vonhof.smartq;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class RedisTaskStore<T extends Task> implements TaskStore<T> {
    private static final Logger log = Logger.getLogger(RedisTaskStore.class);

    private final String LOCK_ID = UUID.randomUUID().toString();

    private ObjectMapper om = new ObjectMapper();
    private final Jedis jedis;

    private final Class<T> taskClass;

    private String queueList = "tasks/queued";
    private String runningList = "tasks/running";
    private String namespace = "smartq/";



    public RedisTaskStore(Jedis jedis,Class<T> taskClass) {
        this.jedis = jedis;
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
        synchronized (jedis) {
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
        }
    }

    private List<T> readTasks(String listId) {
        synchronized (jedis) {
            List<String> ids = jedis.lrange(key(listId), 0, 200);
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
        }
    }

    private void clearList(String listId) {
        synchronized (jedis) {
            List<String> ids = jedis.lrange(key(listId), 0, Integer.MAX_VALUE);
            jedis.del(key(listId));

            if (ids.isEmpty()) {
                return;
            }

            jedis.del(ids.toArray(new String[0]));
        }

    }

    private void writeTask(T task) {
        try {
            synchronized (jedis) {
                jedis.set(docId(task.getId()),om.writeValueAsString(task));
            }
        } catch (IOException e) {
            log.warn("Could not write task doc",e);
        }
    }

    @Override
    public T get(UUID id) {
        return readTask(id);
    }

    @Override
    public void remove(T task) {
        remove(task.getId());
    }

    @Override
    public void remove(UUID id) {
        synchronized (jedis) {
            jedis.del(docId(id));
            jedis.lrem(key(queueList), 100, docId(id));
            jedis.lrem(key(runningList), 100, docId(id));
        }
    }

    @Override
    public void queue(T task) {
        writeTask(task);
        synchronized (jedis) {
            jedis.rpush(key(queueList),docId(task.getId()));
        }

    }

    @Override
    public void run(T task) {
        writeTask(task);
        synchronized (jedis) {
            jedis.lrem(key(queueList),100,docId(task.getId()));
            jedis.rpush(key(runningList),docId(task.getId()));
        }
    }

    @Override
    public List<T> getQueued() {
        return readTasks(queueList);
    }

    @Override
    public List<T> getRunning() {
        return readTasks(runningList);
    }

    @Override
    public long queueSize() {
        synchronized (jedis) {
            return jedis.llen(key(queueList));
        }
    }

    @Override
    public long runningCount() {
        synchronized (jedis) {
            return jedis.llen(key(runningList));
        }
    }

    @Override
    public void unlock() {
        synchronized (jedis) {
            String lock = jedis.get(key("lock"));
            if (lock != null && lock.equalsIgnoreCase(LOCK_ID)) {
                jedis.del(key("lock"));
            }
        }
    }

    @Override
    public void lock() {

        while(true) {
            String lock = null;
            synchronized (jedis) {
                lock = jedis.get(key("lock"));
            }
            if (lock != null) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    log.warn("Interrupted during lock",e);
                    return;
                }
                continue;
            }

            synchronized (jedis) {
                jedis.set(key("lock"),LOCK_ID);
                jedis.expire(key("lock"),30);
            }
            break;
        }

    }

    @Override
    public void waitForChange() throws InterruptedException {

        synchronized (this) {
            ChangeSubscriber subscriber = new ChangeSubscriber(this);

            synchronized (jedis) {
                jedis.subscribe(subscriber,key("changes"));
            }

            wait();
        }
    }

    @Override
    public void signalChange() {
        synchronized (jedis) {
            jedis.publish(key("changes"),"changed");
        }
    }

    public void reset() {
        clearList(queueList);
        clearList(runningList);
        synchronized (jedis) {
            jedis.del(key("changes"));
            jedis.del(key("lock"));
        }
    }

    private class ChangeSubscriber extends JedisPubSub {

        private final TaskStore<T> store;

        public ChangeSubscriber(TaskStore<T> store) {
             this.store = store;
        }

        @Override
        public void onMessage(String channel, String message) {
            unsubscribe();
            synchronized (store) {
                store.notifyAll();
            }
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {}

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {}

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {}

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {}

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {}
    }
}
