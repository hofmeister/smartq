package com.vonhof.smartq;


import com.vonhof.smartq.Task.State;
import org.junit.Test;

import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


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

        assertEquals(1,queue.queueSize());

        assertNotNull(queue.acquire());

        assertEquals(0,queue.queueSize());
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


        assertEquals(6,queue.queueSize());

        assertEquals("Task with highest prio comes first",10,queue.acquire().getPriority());
    }

    @Test
    public void tasks_can_be_cancelled() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.queueSize(),1);

        queue.cancel(task);

        assertEquals(queue.queueSize(),0);

        assertEquals(task.getState(),Task.State.PENDING);
    }

    @Test
    public void tasks_can_fail() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.queueSize(),1);

        queue.failed(task.getId());

        assertEquals(queue.queueSize(),0);

        assertEquals(queue.getStore().get(task.getId()).getState(), State.ERROR);
    }

    @Test
    public void tasks_can_retry_if_failed() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.setMaxRetries("test", 2);
        queue.submit(task);

        assertEquals(queue.queueSize(),1);

        queue.failed(task.getId());

        assertEquals(queue.queueSize(),1);

        assertEquals(queue.getStore().get(task.getId()).getState(), State.PENDING);
    }


    @Test
    public void tasks_can_be_rescheduled() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.queueSize(),1);

        queue.cancel(task, true);

        assertEquals(queue.queueSize(),1);

        task = queue.acquire();

        assertEquals(task.getState(),Task.State.RUNNING);
    }


    @Test
    public void tasks_can_be_rate_limited_by_type() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setRateLimit("test", 2);

        Task task1 = new Task("test");
        Task task2 = new Task("test");
        Task task3 = new Task("test");
        Task task4 = new Task("test");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.queueSize(),4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();

        assertEquals(queue.queueSize(),2);

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

        queue.setRateLimit("test", 2);

        Task task1 = new Task("test").withPriority(4);
        Task task2 = new Task("test").withPriority(3);
        Task task3 = new Task("test2").withPriority(2);
        Task task4 = new Task("test").withPriority(1);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.queueSize(),4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();
        Task running3 = queue.acquire();

        assertEquals(queue.queueSize(),1);
        assertEquals("test2",running3.getTags().first());

        ThreadedRunner runner = new ThreadedRunner(queue);
        runner.start();

        assertFalse("Thread is waiting for ack", runner.isDone());

        queue.acknowledge(running1.getId());

        runner.join();

        assertTrue("Thread is done", runner.isDone());
    }

    @Test
    public void tasks_with_multiple_rate_limited_tags_gets_the_most_restricted() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setRateLimit("test", 2);
        queue.setRateLimit("test2", 4);
        queue.setRateLimit("test3", 10);

        Task task1 = new Task("test")
                .withTag("test2")
                .withTag("test3")
                .withPriority(4);

        assertEquals(2, queue.getRateLimit(task1));
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

        assertEquals(queue.queueSize(),4);

        assertEquals("ETA is correct",6000L,queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type",4000L,queue.getEstimatedTimeLeft("b"));
    }

    @Test
    public void can_do_subscriber_based_estimations() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a",1000);
        Task task2 = new Task("a",1000);
        Task task3 = new Task("b",2000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setSubscribers(2);

        assertEquals(queue.queueSize(), 4);

        assertEquals("ETA is correct",3000L,queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type",1000L,queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type",2000L,queue.getEstimatedTimeLeft("b"));
    }

    @Test
    public void can_get_running_tasks_by_type() throws InterruptedException {
        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a",1000);
        Task task2 = new Task("a",1000);
        Task task3 = new Task("b",2000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        Task a = queue.acquire("a");

        assertFalse("No running b tasks",queue.getStore().getRunning("b").hasNext());

        assertTrue("Can get running A tasks",queue.getStore().getRunning("a").hasNext());

        assertEquals("Can get running A task count",1, queue.getStore().runningCount("a"));

        queue.acknowledge(a.getId());

        assertFalse("No more running A tasks",queue.getStore().getRunning("a").hasNext());

        assertEquals("Can get new running A task count",0, queue.getStore().runningCount("a"));
    }


    @Test
    public void can_do_rate_limited_subscriber_based_estimations() throws InterruptedException {

        WatchProvider.currentTime(0); //Override time - to have better control

        SmartQ<Task,DefaultTaskResult> queue = makeQueue();

        queue.setRateLimit("a", 1);

        Task task1 = new Task("a",1000);
        Task task2 = new Task("b",2000);
        Task task3 = new Task("a",1000);
        Task task4 = new Task("b",2000);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setSubscribers(2);

        assertEquals(queue.queueSize(),4);

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

        queue1.setRateLimit("test", 1);
        queue2.setRateLimit("test", 1);
        queue3.setRateLimit("test", 1);

        Task task1 = new Task("test",1000);
        Task task2 = new Task("test",2000);
        Task task3 = new Task("test",1000);
        Task task4 = new Task("test",2000);

        queue1.submit(task1);
        queue1.submit(task2);
        queue1.submit(task3);
        queue1.submit(task4);

        assertEquals(queue1.queueSize(),4);
        assertEquals(queue2.queueSize(),4);
        assertEquals(queue3.queueSize(),4);

        long queueSize = queue1.queueSize();

        Stack<ThreadedSubscriber> subscribers = new Stack<ThreadedSubscriber>();
        subscribers.add(new ThreadedSubscriber(queue1, "queue1"));
        subscribers.add(new ThreadedSubscriber(queue2, "queue2"));
        subscribers.add(new ThreadedSubscriber(queue3, "queue3"));
        subscribers.add(new ThreadedSubscriber(queue1, "queue4"));

        //Start all subscribers
        for(ThreadedSubscriber subscriber : subscribers) {
            subscriber.start();
        }

        int maxRetries = 10;
        int retries = 0;

        while(!subscribers.isEmpty()) {

            ThreadedSubscriber subscriberWithAcquire = null;

            for(ThreadedSubscriber subscriber : subscribers) {
                if (subscriber.hasAcquired()) {
                    subscriberWithAcquire = subscriber;
                    break;
                }
            }

            if (subscriberWithAcquire == null) {
                Thread.sleep(100);
                retries++;
                if (retries > maxRetries) {
                    fail("Failed to find subscriber that acquired task");
                }
                continue;
            }

            retries = 0;

            subscribers.remove(subscriberWithAcquire);

            for(ThreadedSubscriber subscriber : subscribers) {
                assertFalse("Other subscriber is waiting for ack", subscriber.hasAcquired());
            }

            subscriberWithAcquire.join();

            assertEquals("Queue is decreased by 1", --queueSize, queue1.queueSize());
            assertEquals("Queue has 1 running task", 1, queue1.runningCount());

            queue1.acknowledge(subscriberWithAcquire.getTask().getId());
        }
    }

    @Test
    public void can_wait_and_wakeup() throws InterruptedException {
        TaskStore<Task> store = makeStore();

        ThreadedWaiter locker = new ThreadedWaiter(store);

        locker.start();

        assertFalse("Locker is not done - waiting for signal", locker.isDone());

        Thread.sleep(500);

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
                e.printStackTrace();
            }
        }

        private boolean isDone() {
            return done;
        }
    }

    private static class ThreadedSubscriber extends Thread {
        private final SmartQ<Task,DefaultTaskResult> queue;
        private boolean done = false;
        private Task task;

        private ThreadedSubscriber(SmartQ<Task, DefaultTaskResult> queue, String name) {
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

        private boolean hasAcquired() {
            return done;
        }

        private Task getTask() {
            return task;
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
