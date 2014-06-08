package com.vonhof.smartq.server;


import com.vonhof.smartq.PostgresTaskStore;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.TaskStore;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class PostgresClientServerTest extends ClientServerTest {

    private PostgresTaskStore store = null;

    @Before
    public void setup() throws IOException, SQLException {
        store = new PostgresTaskStore(Task.class);
        store.setTableName("queue_"+ UUID.randomUUID().toString().replaceAll("-", ""));
        store.connect();
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
