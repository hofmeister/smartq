package com.vonhof.smartq;


import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class PostgresQueueTest extends SmartQTest {


    private PostgresTaskStore store = null;

    @Before
    public void setup() throws IOException, SQLException {
        store = new PostgresTaskStore(Task.class);
        store.setTableName("queue_"+ UUID.randomUUID().toString().replaceAll("-", ""));
        store.createTable();
    }

    @After
    public void tearDown() throws Exception {
        store.dropTable();
        store.close();
    }

    protected TaskStore makeStore()  {
        return store;
    }

}
