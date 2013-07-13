package com.vonhof.smartq;


import org.junit.Test;

import static org.junit.Assert.*;


public class SmartQTest {

    protected TaskStore<Task> makeStore() {
        return new MemoryTaskStore<Task>();
    }

    protected SmartQ<Task,DefaultTaskResult> makeQueue() {
        return new SmartQ<Task, DefaultTaskResult>(makeStore());
    }

    protected SmartQ<Task,DefaultTaskResult> makeNode(TaskStore<Task> store) {
        return new SmartQ<Task, DefaultTaskResult>(store);
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
    public void tasks_can_be_prioritized() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task notImportantTask1 = new Task("test").withPriority(1);
        Task notImportantTask2 = new Task("test").withPriority(1);
        Task notImportantTask3 = new Task("test").withPriority(1);
        Task notImportantTask4 = new Task("test").withPriority(1);
        Task notImportantTask5 = new Task("test").withPriority(1);
        Task importantTask = new Task("test").withPriority(10);

        queue.submit(notImportantTask1);
        queue.submit(notImportantTask2);
        queue.submit(notImportantTask3);
        queue.submit(importantTask);
        queue.submit(notImportantTask4);
        queue.submit(notImportantTask5);


        assertEquals(6,queue.size());

        assertEquals("Task with highest prio comes first",10,queue.acquire().getPriority());
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

        Task task1 = new Task("test").withPriority(4);
        Task task2 = new Task("test").withPriority(3);
        Task task3 = new Task("test2").withPriority(2);
        Task task4 = new Task("test").withPriority(1);

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


    @Test
    public void queue_handles_concurrent_acquire() throws InterruptedException {
        final TaskStore<Task> store = makeStore();

        SmartQ<Task,DefaultTaskResult> queue1 = makeNode(store);
        SmartQ<Task,DefaultTaskResult> queue2 = makeNode(store);
        SmartQ<Task,DefaultTaskResult> queue3 = makeNode(store);

        queue1.setTaskTypeRateLimit("test",1);
        queue2.setTaskTypeRateLimit("test",1);
        queue3.setTaskTypeRateLimit("test",1);

        Task task1 = new Task("test",1000);
        Task task2 = new Task("test",2000);
        Task task3 = new Task("test",1000);
        Task task4 = new Task("test",2000);

        queue1.submit(task1);
        queue1.submit(task2);
        queue1.submit(task3);
        queue1.submit(task4);

        assertEquals(queue1.size(),4);
        assertEquals(queue2.size(),4);
        assertEquals(queue3.size(),4);

        ThreadedConsumer consumer1 = new ThreadedConsumer(queue1,"queue1");
        ThreadedConsumer consumer2 = new ThreadedConsumer(queue2,"queue2");
        ThreadedConsumer consumer3 = new ThreadedConsumer(queue3,"queue3");

        consumer1.start();
        consumer1.join();

        consumer2.start();

        assertTrue("Consumer 1 is done", consumer1.isDone());
        assertFalse("Consumer 2 is waiting for ack", consumer2.isDone());
        assertFalse("Consumer 3 is waiting for ack",consumer3.isDone());

        queue2.acknowledge(consumer1.getTask().getId());

        assertEquals(queue3.size(),3);

        consumer2.join();

        consumer3.start();

        assertTrue("Consumer 2 is done", consumer2.isDone());
        assertFalse("Consumer 3 is waiting for ack",consumer3.isDone());

        queue3.acknowledge(consumer2.getTask().getId());

        assertEquals(queue3.size(),2);

        consumer3.join();
        assertTrue("Consumer 3 is done", consumer3.isDone());


    }

    private static class ThreadedConsumer extends Thread {
        private final SmartQ<Task,DefaultTaskResult> queue;
        private boolean done = false;
        private Task task;

        private ThreadedConsumer(SmartQ<Task, DefaultTaskResult> queue,String name) {
            super(name);
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                task = queue.acquire();
                done = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private boolean isDone() {
            return done;
        }

        private Task getTask() {
            return task;
        }
    }

    @Test
    public void can_lock_and_unlock() throws InterruptedException {
        TaskStore<Task> store = makeStore();

        ThreadedLocker locker = new ThreadedLocker(store);
        store.lock();

        locker.start();

        assertFalse("Locker is not done - waiting for unlock",locker.isDone());

        store.unlock();

        locker.join();

        assertTrue("Locker is done", locker.isDone());
    }

    @Test
    public void can_wait_and_wakeup() throws InterruptedException {
        TaskStore<Task> store = makeStore();

        ThreadedLocker locker = new ThreadedLocker(store);

        locker.start();

        assertFalse("Locker is not done - waiting for signal", locker.isDone());

        store.signalChange();

        locker.join();

        assertTrue("Locker is done", locker.isDone());
    }

    private static class ThreadedWaiter extends Thread {
        private final TaskStore<Task> store;
        private boolean done;

        private ThreadedWaiter(TaskStore<Task> store) {
            this.store = store;
        }

        @Override
        public void run() {
            try {
                store.waitForChange();
                done = true;
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        private boolean isDone() {
            return done;
        }
    }

    private static class ThreadedLocker extends Thread {
        private final TaskStore<Task> store;
        private boolean done;

        private ThreadedLocker(TaskStore<Task> store) {
            this.store = store;
        }

        @Override
        public void run() {
            store.lock();
            done = true;
        }

        private boolean isDone() {
            return done;
        }
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
