package com.vonhof.smartq;


import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class WriteThroughQueueTest extends SmartQTest {


    private PostgresTaskStore pgStore;
    private WriteThroughTaskStore store;


    @Before
    public void setup() throws IOException, SQLException {
        pgStore = new PostgresTaskStore(Task.class);
        pgStore.setTableName("queue_" + UUID.randomUUID().toString().replaceAll("-", ""));
        pgStore.connect();
        pgStore.createTable();

        store = new WriteThroughTaskStore(pgStore);
    }

    @After
    public void tearDown() throws Exception {
        pgStore.dropTable();
        store.close();
    }

    protected TaskStore makeStore()  {
        return store;
    }

}
