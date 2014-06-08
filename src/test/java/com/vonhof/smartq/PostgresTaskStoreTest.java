package com.vonhof.smartq;


import org.junit.After;

import java.util.UUID;

public class PostgresTaskStoreTest extends TaskStoreTest {

    @After
    public void tearDown() throws Exception {
        PostgresTaskStore pgStore = (PostgresTaskStore) store;
        pgStore.dropTable();
        pgStore.close();
    }

    @Override
    protected TaskStore makeStore()  {
        try {
            PostgresTaskStore store = new PostgresTaskStore(Task.class);
            store.setTableName("queue_"+UUID.randomUUID().toString().replaceAll("-", ""));
            store.connect();
            store.createTable();
            return store;
        } catch (Exception e) {
            e.printStackTrace();

        }
        throw new AssertionError("Could not create pg task store");
    }

}
