package com.vonhof.smartq;


import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class WriteThroughQueueTest extends SmartQTest {


    private PostgresTaskStore pgStore = null;

    @Before
    public void setup() throws IOException, SQLException {
        pgStore = new PostgresTaskStore(Task.class);
        pgStore.setTableName("queue_" + UUID.randomUUID().toString().replaceAll("-", ""));
        pgStore.createTable();
    }

    @After
    public void tearDown() throws Exception {
        pgStore.dropTable();
        pgStore.close();
    }

    protected TaskStore makeStore()  {
        return new WriteThroughTaskStore(pgStore);
    }

}
