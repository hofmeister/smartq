package com.vonhof.smartq;


import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class WriteThroughTaskStoreTest extends TaskStoreTest {

    private PostgresTaskStore pgStore;

    @After
    public void tearDown() throws Exception {
        pgStore.dropTable();
    }

    @Before
    public void setup() throws SQLException, IOException {
        pgStore = new PostgresTaskStore(Task.class);
        pgStore.setTableName("queue_"+UUID.randomUUID().toString().replaceAll("-", ""));
        pgStore.createTable();
    }

    @Override
    protected TaskStore makeStore()  {
        return new WriteThroughTaskStore(pgStore);
    }

}
