package com.vonhof.smartq.server;


import com.vonhof.smartq.PostgresTaskStore;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.TaskStore;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class PostgresConsumerProducerTest extends ConsumerProducerTest {

    private PostgresTaskStore<Task> store = null;

    @Before
    public void setup() throws IOException, SQLException {
        store = new PostgresTaskStore<Task>(Task.class);
        store.setTableName("queue_"+ UUID.randomUUID().toString().replaceAll("-", ""));
        store.createTable();
    }

    @After
    public void tearDown() throws SQLException {
        store.dropTable();
        store.close();
    }

    protected TaskStore<Task> makeStore()  {
        return store;
    }
}
