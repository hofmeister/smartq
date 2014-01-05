package com.vonhof.smartq.server;

import com.vonhof.smartq.MemoryTaskStore;
import com.vonhof.smartq.PostgresTaskStore;
import com.vonhof.smartq.RedisTaskStore;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.TaskStore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Stand-alone SmartQ server
 */
public class StandAlone {

    public static void main(String[] args) throws IOException, SQLException {
        String configFile = "smartq.properties";

        if (args.length > 0) {
            configFile = args[0];
        }

        Properties props = new Properties();
        props.load(new FileInputStream(new File(configFile)));

        InetSocketAddress serverAddress = new InetSocketAddress(
                props.getProperty("bind.host","localhost"),
                Integer.valueOf(props.getProperty("bind.port","51765")));

        String storeType = props.getProperty("store.type", "pg");

        TaskStore<Task> taskStore = null;
        if ("pg".equalsIgnoreCase(storeType)) {
            taskStore = new PostgresTaskStore<Task>(Task.class,
                    props.getProperty("pg.url","jdbc:postgresql://localhost/smartq"),
                    props.getProperty("pg.username","postgres"),
                    props.getProperty("pg.password",""));
        } else if ("redis".equalsIgnoreCase(storeType)) {
            final JedisPool jedis = new JedisPool(new JedisPoolConfig(),
                    props.getProperty("redis.host","localhost"),
                    Integer.valueOf(props.getProperty("redis.port","6379")),
                    0);

            RedisTaskStore<Task> store = new RedisTaskStore<Task>(jedis, Task.class);

            store.setNamespace(props.getProperty("redis.namespace","smartq")+"/");
            taskStore = new RedisTaskStore<Task>(jedis, Task.class);
        } else if ("memory".equalsIgnoreCase(storeType) || storeType.isEmpty()) {
            taskStore = new MemoryTaskStore<Task>();
        } else {
            throw new IllegalArgumentException("Unknown task store type: " + storeType);
        }

        SmartQ<Task,Serializable> smartQ = new SmartQ<Task,Serializable>(taskStore);

        SmartQServer<Task> server = new SmartQServer<Task>(serverAddress, smartQ);

        server.listen();
    }

}
