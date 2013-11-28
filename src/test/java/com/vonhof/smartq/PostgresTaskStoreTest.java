package com.vonhof.smartq;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PostgresTaskStoreTest {

    private PostgresTaskStore<Task> store = null;

    @Before
    public void setup() throws IOException, SQLException {
        store = new PostgresTaskStore<Task>(Task.class);
        store.setTableName("queue_"+UUID.randomUUID().toString().replaceAll("-", ""));
        store.createTable();
    }

    @After
    public void tearDown() throws SQLException {
        store.dropTable();
    }

    private PostgresTaskStore<Task> makeStore()  {
        return store;
    }

    @Test
    public void can_add_and_remove() throws SQLException {
        PostgresTaskStore<Task> store = makeStore();

        assertEquals("SmartQ is empty",0,store.queueSize());
        assertEquals("Running is empty",0,store.runningCount());


        Task t = new Task("test");
        store.queue(t);

        assertEquals("Task is stored",1,store.queueSize());

        assertNotNull("Task can be fetched",store.get(t.getId()));

        store.run(t);

        assertEquals("Task is moved to running list",0,store.queueSize());
        assertEquals("Task is moved to running list",1,store.runningCount());

        assertNotNull("Task can be fetched",store.get(t.getId()));

        store.remove(t);

        assertEquals("Task can be removed",0,store.queueSize());
        assertEquals("Task can be removed",0,store.runningCount());

        assertNull("Task can not be fetched", store.get(t.getId()));
    }



}
