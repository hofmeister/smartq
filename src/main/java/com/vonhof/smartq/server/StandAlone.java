package com.vonhof.smartq.server;

import com.vonhof.smartq.*;

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
