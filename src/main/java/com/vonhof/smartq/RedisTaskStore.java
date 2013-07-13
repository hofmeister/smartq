package com.vonhof.smartq;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class RedisTaskStore<T extends Task> implements TaskStore<T> {
    private static final Logger log = Logger.getLogger(RedisTaskStore.class);

    private ObjectMapper om = new ObjectMapper();
    private final Jedis jedis;

    private final Class<T> taskClass;

    private String queueList = "tasks/queued";
    private String runningList = "tasks/running";
    private String namespace = "";


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

    private List<T> readTasks(String listId) {
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

    private void clearList(String listId) {
        List<String> ids = jedis.lrange(key(listId), 0, Integer.MAX_VALUE);
        jedis.del(key(listId));

        if (ids.isEmpty()) {
            return;
        }

        jedis.del(ids.toArray(new String[0]));

    }

    private void writeTask(T task) {
        try {
            jedis.set(docId(task.getId()),om.writeValueAsString(task));
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
        jedis.del(docId(id));
        jedis.lrem(key(queueList), 100, docId(id));
        jedis.lrem(key(runningList), 100, docId(id));
    }

    @Override
    public void queue(T task) {
        writeTask(task);
        jedis.rpush(key(queueList),docId(task.getId()));
    }

    @Override
    public void run(T task) {
        writeTask(task);
        jedis.lrem(key(queueList),100,docId(task.getId()));
        jedis.rpush(key(runningList),docId(task.getId()));
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
        return jedis.llen(key(queueList));
    }

    @Override
    public long runningCount() {
        return jedis.llen(key(runningList));
    }

    public void reset() {
        clearList(queueList);
        clearList(runningList);
    }
}
