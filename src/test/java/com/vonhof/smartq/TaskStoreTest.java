package com.vonhof.smartq;

import com.vonhof.smartq.Task.State;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskStoreTest {

    protected TaskStore store = null;

    @Before
    public void setup() throws IOException, SQLException {
        store = makeStore();
    }


    protected TaskStore makeStore()  {
        return new MemoryTaskStore();
    }

    @Test
    public void can_add_and_remove() throws SQLException, InterruptedException {
        TaskStore store = makeStore();

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

    @Test
    public void task_can_be_marked_as_failed() throws SQLException, InterruptedException {
        TaskStore store = makeStore();

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

        store.failed(t);

        assertEquals("Task can be marked as failed",0,store.queueSize());
        assertEquals("Task can be marked as failed",0,store.runningCount());

        assertNotNull("Task can be fetched",store.get(t.getId()));
        assertEquals("Task has expected state", State.ERROR, store.get(t.getId()).getState());
        assertTrue("Failed tasks can be iterated", store.getFailed().hasNext());
    }

    @Test
    public void can_get_first_and_last_task_with_same_reference()  {
        TaskStore store = makeStore();

        Task firstA = new Task("1").withReferenceId("a");
        Task lastB = new Task("2").withReferenceId("b");

        Task a1 = new Task("2").withReferenceId("a");
        Task a2 = new Task("3").withReferenceId("a");
        Task lastA = new Task("4").withReferenceId("a");

        Task b1 = new Task("5").withReferenceId("b").withPriority(2);
        Task b2 = new Task("6").withReferenceId("b").withPriority(2);

        Task firstB = new Task("7").withReferenceId("b").withPriority(5);

        store.queue(firstA);
        store.queue(lastB);
        store.queue(a1);
        store.queue(a2);
        store.queue(lastA);
        store.queue(b1);
        store.queue(b2);
        store.queue(firstB);

        assertEquals(firstA, store.getFirstTaskWithReference("a"));
        assertEquals(lastA, store.getLastTaskWithReference("a"));

        assertEquals(firstB, store.getFirstTaskWithReference("b"));
        assertEquals(lastB, store.getLastTaskWithReference("b"));
    }
}
