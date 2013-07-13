package com.vonhof.smartq;


import org.junit.Test;

import static org.junit.Assert.*;


public class SmartQTest {

    protected SmartQ<Task,DefaultTaskResult> makeQueue() {
        return new SmartQ<Task, DefaultTaskResult>(new MemoryTaskStore<Task>());

    }

    @Test
    public void tasks_can_be_added_and_acquired() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(1,queue.size());

        assertNotNull(queue.acquire());

        assertEquals(0,queue.size());
    }

    @Test
    public void tasks_can_be_cancelled() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.size(),1);

        queue.cancel(task);

        assertEquals(queue.size(),0);

        assertEquals(task.getState(),Task.State.PENDING);
    }

    @Test
    public void tasks_can_be_rate_limited_by_type() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setTaskTypeRateLimit("test",2);

        Task task1 = new Task("test");
        Task task2 = new Task("test");
        Task task3 = new Task("test");
        Task task4 = new Task("test");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.size(),4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();

        assertEquals(queue.size(),2);

        ThreadedRunner runner = new ThreadedRunner(queue);
        runner.start();

        assertFalse("Thread is waiting for ack",runner.isDone());

        queue.acknowledge(running1.getId());

        runner.join();

        assertTrue("Thread is done", runner.isDone());
    }

    @Test
    public void tasks_with_different_type_is_not_rate_limited() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setTaskTypeRateLimit("test",2);

        Task task1 = new Task("test2");
        Task task2 = new Task("test");
        Task task3 = new Task("test");
        Task task4 = new Task("test");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.size(),4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();
        Task running3 = queue.acquire();

        assertEquals(queue.size(),1);
        assertEquals("test2",running3.getType());

        ThreadedRunner runner = new ThreadedRunner(queue);
        runner.start();

        assertFalse("Thread is waiting for ack", runner.isDone());

        queue.acknowledge(running1.getId());

        runner.join();

        assertTrue("Thread is done", runner.isDone());
    }

    @Test
    public void can_do_simple_estimations() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a",1000);
        Task task2 = new Task("a",1000);
        Task task3 = new Task("b",2000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.size(),4);

        assertEquals("ETA is correct",6000L,queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type",4000L,queue.getEstimatedTimeLeft("b"));
    }

    @Test
    public void can_do_consumer_based_estimations() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a",1000);
        Task task2 = new Task("a",1000);
        Task task3 = new Task("b",2000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setConsumers(2);

        assertEquals(queue.size(), 4);

        assertEquals("ETA is correct",3000L,queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type",1000L,queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("b"));
    }


    @Test
    public void can_do_rate_limited_consumer_based_estimations() throws InterruptedException {

        WatchProvider.currentTime(0); //Override time - to have better control

        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setTaskTypeRateLimit("a",1);

        Task task1 = new Task("a",1000);
        Task task2 = new Task("b",2000);
        Task task3 = new Task("a",1000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setConsumers(2);

        assertEquals(queue.size(),4);

        assertEquals("ETA is correct",4000L,queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("b"));

        Task a = queue.acquire("a");

        WatchProvider.appendTime(500); //Simulate 500 ms passing

        assertEquals("ETA accounts for time running", 3500L, queue.getEstimatedTimeLeft());

        queue.acknowledge(a.getId());

        assertEquals("ETA is updated when task is ack'ed", 3000L, queue.getEstimatedTimeLeft());

    }


    private static class ThreadedRunner extends Thread {
        private final SmartQ<Task,DefaultTaskResult> queue;
        private boolean done;

        private ThreadedRunner(SmartQ<Task, DefaultTaskResult> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                Task task = queue.acquire();
                queue.acknowledge(task.getId());
                done = true;

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private boolean isDone() {
            return done;
        }
    }

}
