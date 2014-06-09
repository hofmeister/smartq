package com.vonhof.smartq;


import com.vonhof.smartq.Task.State;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class SmartQTest {

    protected TaskStore makeStore() {
        return new MemoryTaskStore();
    }

    protected SmartQ<DefaultTaskResult> makeQueue() {
        return new SmartQ<DefaultTaskResult>(makeStore());
    }

    protected SmartQ<DefaultTaskResult> makeNode(TaskStore store) {
        return new SmartQ<DefaultTaskResult>(store);
    }


    @Test
    public void concurrency_is_calculated_based_on_tag_and_global_setting() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        assertEquals(-1, queue.getConcurrency());
        assertEquals(-1, queue.getConcurrency("any"));

        assertFalse(queue.isRateLimited(new Task("any")));

        queue.setRateLimit("any", 5);


        assertEquals(-1, queue.getConcurrency());
        assertEquals(5, queue.getConcurrency("any"));
        assertFalse(queue.isRateLimited(new Task("any")));

    }


    @Test
    public void tasks_can_be_added_and_acquired() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(1, queue.queueSize());

        assertNotNull(queue.acquire());

        assertEquals(0, queue.queueSize());
    }


    @Test
    public void tasks_can_be_prioritized() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

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


        assertEquals(6, queue.queueSize());

        assertEquals("Task with highest prio comes first", 10, queue.acquire().getPriority());
    }

    @Test
    public void tasks_can_be_cancelled() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.queueSize(), 1);

        queue.cancel(task);

        assertEquals(queue.queueSize(), 0);

        assertEquals(task.getState(), Task.State.PENDING);
    }

    @Test
    public void tasks_can_fail() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        Thread.sleep(500); //Allow async ops to go through

        assertEquals(queue.queueSize(), 1);

        queue.failed(task.getId());

        assertEquals(queue.queueSize(), 0);

        assertEquals(State.ERROR, queue.getStore().get(task.getId()).getState());
    }

    @Test
    public void tasks_can_retry_if_failed() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.setMaxRetries("test", 2);
        queue.submit(task);

        assertEquals(queue.queueSize(), 1);

        queue.failed(task.getId());

        assertEquals(queue.queueSize(), 1);

        assertEquals(queue.getStore().get(task.getId()).getState(), State.PENDING);
    }


    @Test
    public void tasks_can_be_rescheduled() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task = new Task("test");

        queue.submit(task);

        assertEquals(queue.queueSize(), 1);

        queue.cancel(task, true);

        assertEquals(queue.queueSize(), 1);

        task = queue.acquire();

        assertEquals(task.getState(), Task.State.RUNNING);
    }


    @Test
    public void tasks_can_be_rate_limited_by_type() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("test");
        Task task2 = new Task("test");
        Task task3 = new Task("test");
        Task task4 = new Task("test");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setRateLimit("test", 2);

        assertEquals(queue.queueSize(), 4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();

        assertEquals(queue.queueSize(), 2);

        ThreadedRunner runner = new ThreadedRunner(queue);
        runner.start();

        assertFalse("Thread is waiting for ack", runner.isDone());

        queue.acknowledge(running1.getId());

        runner.join();

        assertTrue("Thread is done", runner.isDone());
    }

    @Test
    public void tasks_with_different_type_is_not_rate_limited() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        queue.setRateLimit("test", 2);

        Task task1 = new Task("test").withPriority(4);
        Task task2 = new Task("test").withPriority(3);
        Task task3 = new Task("test2").withPriority(2);
        Task task4 = new Task("test").withPriority(1);

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        assertEquals(queue.queueSize(), 4);

        Task running1 = queue.acquire();
        Task running2 = queue.acquire();
        Task running3 = queue.acquire();

        assertEquals(queue.queueSize(), 1);
        assertEquals("test2", running3.getTagSet().iterator().next());

        ThreadedRunner runner = new ThreadedRunner(queue);
        runner.start();

        assertFalse("Thread is waiting for ack", runner.isDone());

        queue.acknowledge(running1.getId());

        runner.join();

        assertTrue("Thread is done", runner.isDone());
    }

    @Test
    public void tasks_with_multiple_rate_limited_tags_gets_the_most_restricted() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

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
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        Task task2 = new Task("a");
        Task task3 = new Task("b");
        Task task4 = new Task("b");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);
        queue.setSubscribers(1);
        queue.setEstimateForTaskType("a", 1000);
        queue.setEstimateForTaskType("b", 2000);

        assertEquals(queue.queueSize(), 4);

        assertEquals("ETA is correct", 6000L, queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type", 4000L, queue.getEstimatedTimeLeft("b"));
    }

    @Test
    public void can_do_subscriber_based_estimations() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        Task task2 = new Task("a");
        Task task3 = new Task("b");
        Task task4 = new Task("b");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);
        queue.setEstimateForTaskType("a", 1000);
        queue.setEstimateForTaskType("b", 2000);

        queue.setSubscribers(2);

        assertEquals(queue.queueSize(), 4);

        assertEquals("ETA is correct", 3000L, queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type", 1000L, queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("b"));
    }

    @Test
    public void can_get_running_tasks_by_type() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        Task task2 = new Task("a");
        Task task3 = new Task("b");
        Task task4 = new Task("b");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setEstimateForTaskType("a", 1000);
        queue.setEstimateForTaskType("b", 2000);

        Task a = queue.acquire("a");

        assertFalse("No running b tasks", queue.getStore().getRunning("b").hasNext());

        assertTrue("Can get running A tasks", queue.getStore().getRunning("a").hasNext());

        assertEquals("Can get running A task count", 1, queue.getStore().runningCount("a"));

        queue.acknowledge(a.getId());

        assertFalse("No more running A tasks", queue.getStore().getRunning("a").hasNext());

        assertEquals("Can get new running A task count", 0, queue.getStore().runningCount("a"));
    }


    @Test
    public void estimates_are_updated_as_tasks_are_executed() throws InterruptedException {

        WatchProvider.currentTime(0); //Override time - to have better control

        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        Task task2 = new Task("a");
        Task task3 = new Task("a");
        Task task4 = new Task("a");
        Task task5 = new Task("a");
        Task task6 = new Task("a");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);
        queue.submit(task5);
        queue.submit(task6);

        queue.setRateLimit("a", 1);
        queue.setSubscribers(2);
        queue.setEstimateForTaskType("a", 1000);

        assertEquals("Estimate is as expected", 1000L, queue.getEstimateForTaskType("a"));

        Task a = queue.acquire("a");
        WatchProvider.appendTime(500);
        queue.acknowledge(a.getId());

        assertEquals("Estimate shows updated time", 750L, queue.getEstimateForTaskType("a"));

        Task a2 = queue.acquire("a");
        WatchProvider.appendTime(1500);
        queue.acknowledge(a2.getId());

        assertEquals("Estimate shows updated time", 1000L, queue.getEstimateForTaskType("a"));

    }


    @Test
    public void can_do_rate_limited_subscriber_based_estimations() throws InterruptedException {

        WatchProvider.currentTime(0); //Override time - to have better control

        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        Task task2 = new Task("b");
        Task task3 = new Task("a");
        Task task4 = new Task("b");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);
        queue.setEstimateForTaskType("a", 1000);
        queue.setEstimateForTaskType("b", 2000);

        queue.setRateLimit("a", 1);
        queue.setSubscribers(2);

        assertEquals(queue.queueSize(), 4);

        assertEquals("ETA is correct", 4000L, queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("b"));

        Task a = queue.acquire("a");

        WatchProvider.appendTime(500); //Simulate 500 ms passing

        assertEquals("ETA accounts for time running", 3500L, queue.getEstimatedTimeLeft());

        WatchProvider.appendTime(500); //Simulate 500 ms passing
        queue.acknowledge(a.getId());

        assertEquals(1000L, queue.getEstimateForTaskType("a"));

        assertEquals("ETA is updated when task is ack'ed", 3000L, queue.getEstimatedTimeLeft());

    }

    @Test
    public void can_do_rate_limiting_from_task_configuration() throws InterruptedException {

        WatchProvider.currentTime(0); //Override time - to have better control

        SmartQ<DefaultTaskResult> queue = makeQueue();

        Task task1 = new Task("a");
        task1.addRateLimit("sometag",1);
        Task task3 = new Task("a");
        task3.addRateLimit("sometag",1);

        Task task2 = new Task("b");
        Task task4 = new Task("b");

        queue.submit(task1);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task4);

        queue.setEstimateForTaskType("a", 1000);
        queue.setEstimateForTaskType("b", 2000);
        queue.setSubscribers(2);

        assertEquals(queue.queueSize(), 4);

        assertEquals("ETA is correct", 4000L, queue.getEstimatedTimeLeft());

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("a"));

        assertEquals("ETA is correct for type", 2000L, queue.getEstimatedTimeLeft("b"));

        Task a = queue.acquire("a");

        WatchProvider.appendTime(500); //Simulate 500 ms passing

        assertEquals("ETA accounts for time running", 3500L, queue.getEstimatedTimeLeft());

        WatchProvider.appendTime(500); //Simulate 500 ms passing
        queue.acknowledge(a.getId());

        assertEquals(1000L, queue.getEstimateForTaskType("a"));

        assertEquals("ETA is updated when task is ack'ed", 3000L, queue.getEstimatedTimeLeft());

    }

    @Test
    @Ignore //Unignore these to test speed rates
    public void can_submit_tasks_at_high_rates() throws InterruptedException {
        double before = System.currentTimeMillis();
        double amount = 10000;

        SmartQ<DefaultTaskResult> queue = makeQueue();
        for (int i = 0; i < amount; i++) {
            queue.submit(new Task("test"));
        }

        double secs = (System.currentTimeMillis() - before) / 1000;

        double amountPerSec = amount / secs;
        System.out.println(queue.getStore().getClass().getName() + ": Submit rate: " + amountPerSec + " / s - took " + secs + " seconds");

        assertTrue("Has a speed rate above 1000 submits / second", amountPerSec > 500);
    }

    @Test
    @Ignore
    public void can_acquire_tasks_at_high_rates() throws InterruptedException {
        double amount = 10000;
        SmartQ<DefaultTaskResult> queue = makeQueue();
        for (int i = 0; i < amount; i++) {
            queue.submit(new Task("test"));
        }

        double before = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            Task acquire = queue.acquire();
            queue.acknowledge(acquire.getId());
        }

        double secs = (System.currentTimeMillis() - before) / 1000;

        double amountPerSec = amount / secs;
        System.out.println(queue.getStore().getClass().getName() + ": Acquire rate: " + amountPerSec + " / s - took " + secs + " seconds");

        assertTrue("Has a speed rate above 1000 submits / second", amountPerSec > 100);
    }


    @Test
    public void queue_handles_concurrent_acquire() throws InterruptedException {
        final TaskStore store = makeStore();

        SmartQ<DefaultTaskResult> queue1 = makeNode(store);
        SmartQ<DefaultTaskResult> queue2 = makeNode(store);
        SmartQ<DefaultTaskResult> queue3 = makeNode(store);

        Task task1 = new Task("test");
        Task task2 = new Task("test");
        Task task3 = new Task("test");
        Task task4 = new Task("test");

        queue1.submit(task1);
        queue1.submit(task2);
        queue1.submit(task3);
        queue1.submit(task4);

        queue1.setEstimateForTaskType("test", 2000);
        queue1.setRateLimit("test", 1);
        queue2.setRateLimit("test", 1);
        queue3.setRateLimit("test", 1);

        assertEquals(queue1.queueSize(), 4);
        assertEquals(queue2.queueSize(), 4);
        assertEquals(queue3.queueSize(), 4);

        long queueSize = queue1.queueSize();

        Stack<ThreadedSubscriber> subscribers = new Stack<ThreadedSubscriber>();
        subscribers.add(new ThreadedSubscriber(queue1, "queue1"));
        subscribers.add(new ThreadedSubscriber(queue2, "queue2"));
        subscribers.add(new ThreadedSubscriber(queue3, "queue3"));
        subscribers.add(new ThreadedSubscriber(queue1, "queue4"));

        //Start all subscribers
        for (ThreadedSubscriber subscriber : subscribers) {
            subscriber.start();
        }

        int maxRetries = 50;
        int retries = 0;

        while (!subscribers.isEmpty()) {

            ThreadedSubscriber subscriberWithAcquire = null;

            for (ThreadedSubscriber subscriber : subscribers) {
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

            for (ThreadedSubscriber subscriber : subscribers) {
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
        TaskStore store = makeStore();

        ThreadedWaiter locker = new ThreadedWaiter(store);

        locker.start();

        assertFalse("Locker is not done - waiting for signal", locker.isDone());

        Thread.sleep(500);

        store.signalChange();

        locker.join();

        assertTrue("Locker is done", locker.isDone());
    }

    @Test
    public void can_estimate_simple_queues() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("test");
        Task b  = new Task("test");
        Task c  = new Task("test");
        Task d  = new Task("test");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);

        queue.setSubscribers(2);
        queue.setRateLimit("test", 2);
        queue.setEstimateForTaskType("test", 1000L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(2000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a,b,c,d), estimator.getLastExecutionOrder());

        queue.setRateLimit("test", 1);

        assertEquals(4000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a,b,c,d), estimator.getLastExecutionOrder());

        queue.setRateLimit("test", -1);
        queue.setSubscribers(10);

        assertEquals(1000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a, b, c, d), estimator.getLastExecutionOrder());
    }

    @Test
    public void can_estimate_when_a_simple_group_of_tasks_starts_and_ends() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("test").withReferenceId("a");
        Task b  = new Task("test").withReferenceId("b");
        Task c  = new Task("test").withReferenceId("b");
        Task d  = new Task("test").withReferenceId("a");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);

        queue.setSubscribers(1);
        queue.setRateLimit("test", 1);
        queue.setEstimateForTaskType("test", 1000L);


        assertEquals(0L, queue.getEstimatedStartTime("a"));
        assertEquals(4000L, queue.getEstimatedEndTime("a"));

        assertEquals(1000L, queue.getEstimatedStartTime("b"));
        assertEquals(3000L, queue.getEstimatedEndTime("b"));
    }


    @Test
    public void can_estimate_when_a_complex_group_of_tasks_starts_and_ends() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task f  = new Task("tb").withReferenceId("a").withPriority(3);
        Task b  = new Task("ta").withReferenceId("b").withPriority(2);
        Task d  = new Task("tb").withReferenceId("a").withPriority(2);
        Task a  = new Task("ta").withReferenceId("a");
        Task c  = new Task("tb").withReferenceId("b");
        Task e  = new Task("ta").withReferenceId("b");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);
        queue.submit(e);
        queue.submit(f);

        queue.setSubscribers(3);
        queue.setRateLimit("ta", 2);
        queue.setRateLimit("tb", 3);
        queue.setEstimateForTaskType("ta", 2000L);
        queue.setEstimateForTaskType("tb", 1000L);

        assertEquals(0L, queue.getEstimatedStartTime("a"));
        assertEquals(3000L, queue.getEstimatedEndTime("a"));

        assertEquals(0L, queue.getEstimatedStartTime("b"));
        assertEquals(4000L, queue.getEstimatedEndTime("b"));
    }




    @Test
    public void can_estimate_when_a_task_begins_in_simple_queues() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("test");
        Task b  = new Task("test");
        Task c  = new Task("test");
        Task d  = new Task("test");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);

        queue.setSubscribers(2);
        queue.setRateLimit("test", 2);
        queue.setEstimateForTaskType("test", 1000L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(0L, estimator.taskStarts(b));
        assertEquals(1000L, estimator.taskStarts(d));

        queue.setSubscribers(1);

        assertEquals(1000L, estimator.taskStarts(b));
        assertEquals(3000L, estimator.taskStarts(d));


        queue.setSubscribers(10);
        queue.setRateLimit("test",-1);

        assertEquals(0L, estimator.taskStarts(b));
        assertEquals(0L, estimator.taskStarts(d));
    }


    @Test
    public void can_estimate_queues_with_several_rate_limits() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("rate1");
        Task b  = new Task("rate1");
        Task c  = new Task("rate2");
        Task d  = new Task("rate2");
        Task e  = new Task("rate3");
        Task f  = new Task("rate3");
        Task g  = new Task("rate3");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);
        queue.submit(e);
        queue.submit(f);
        queue.submit(g);

        queue.setSubscribers(6);
        queue.setRateLimit("rate1", 1);
        queue.setRateLimit("rate2", 2);
        queue.setRateLimit("rate3", 3);

        queue.setEstimateForTaskType("rate1", 1000L);
        queue.setEstimateForTaskType("rate2", 1000L);
        queue.setEstimateForTaskType("rate3", 1000L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(2000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a, c, d, e, f, g, b), estimator.getLastExecutionOrder());

        queue.setSubscribers(3);

        assertEquals(3000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a, c, d, b, e, f, g), estimator.getLastExecutionOrder());

        queue.setSubscribers(2);

        assertEquals(4000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a, c, b, d, e, f, g), estimator.getLastExecutionOrder());
    }


    @Test
    public void can_estimate_when_a_task_begins_in_a_queue_with_rate_limits() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("rate1");
        Task b  = new Task("rate1");
        Task c  = new Task("rate2");
        Task d  = new Task("rate2");
        Task e  = new Task("rate3");
        Task f  = new Task("rate3");
        Task g  = new Task("rate3");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);
        queue.submit(e);
        queue.submit(f);
        queue.submit(g);

        queue.setSubscribers(6);
        queue.setRateLimit("rate1", 1);
        queue.setRateLimit("rate2", 2);
        queue.setRateLimit("rate3", 3);

        queue.setEstimateForTaskType("rate1", 1000L);
        queue.setEstimateForTaskType("rate2", 1000L);
        queue.setEstimateForTaskType("rate3", 1000L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(1000L, estimator.taskStarts(b));
        assertHashEquals(Arrays.asList(a,c,d,e,f,g), estimator.getLastExecutionOrder());

        queue.setSubscribers(3);

        assertEquals(1000L, estimator.taskStarts(b));
        assertHashEquals(Arrays.asList(a,c,d), estimator.getLastExecutionOrder());

        queue.setSubscribers(2);

        assertEquals(1000L, estimator.taskStarts(b));
        assertHashEquals(Arrays.asList(a, c), estimator.getLastExecutionOrder());
    }


    @Test
    public void can_estimate_queues_with_various_runtimes_and_rate_limits() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("rate1");
        Task b  = new Task("rate1");
        Task c  = new Task("rate2");
        Task d  = new Task("rate2");
        Task e  = new Task("rate3");
        Task f  = new Task("rate3");
        Task g  = new Task("rate3");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);
        queue.submit(e);
        queue.submit(f);
        queue.submit(g);

        queue.setSubscribers(6);
        queue.setRateLimit("rate1", 2);
        queue.setRateLimit("rate2", 1);
        queue.setRateLimit("rate3", 2);

        queue.setEstimateForTaskType("rate1", 2000L);
        queue.setEstimateForTaskType("rate2", 1000L);
        queue.setEstimateForTaskType("rate3", 2500L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(5000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a,b,c,e,f,d,g), estimator.getLastExecutionOrder());

        queue.setSubscribers(3);
        assertEquals(7000L, estimator.queueEnds());
        assertHashEquals(Arrays.asList(a, b, c, d, e, f, g), estimator.getLastExecutionOrder());
    }

    @Test
    public void can_estimate_when_a_task_starts_in_a_queue_with_various_runtimes_and_rate_limits() throws InterruptedException {
        SmartQ<DefaultTaskResult> queue = makeQueue();
        Task a  = new Task("rate1");
        Task b  = new Task("rate1");
        Task c  = new Task("rate2");
        Task d  = new Task("rate2");
        Task e  = new Task("rate3");
        Task f  = new Task("rate3");
        Task g  = new Task("rate3");

        queue.submit(a);
        queue.submit(b);
        queue.submit(c);
        queue.submit(d);
        queue.submit(e);
        queue.submit(f);
        queue.submit(g);

        queue.setSubscribers(6);
        queue.setRateLimit("rate1", 2);
        queue.setRateLimit("rate2", 1);
        queue.setRateLimit("rate3", 2);

        queue.setEstimateForTaskType("rate1", 2000L);
        queue.setEstimateForTaskType("rate2", 1000L);
        queue.setEstimateForTaskType("rate3", 2500L);

        QueueEstimator estimator = new QueueEstimator(queue);

        assertEquals(0L, estimator.taskStarts(e));
        assertHashEquals(Arrays.asList(a,b,c), estimator.getLastExecutionOrder());

        queue.setSubscribers(3);
        assertEquals(2000L, estimator.taskStarts(e));
        assertHashEquals(Arrays.asList(a, b, c, d), estimator.getLastExecutionOrder());
    }


    @Test
    public void can_estimate_a_large_complex_queue() throws InterruptedException {

        //System.out.println("Grace period");
        //Thread.sleep(5000);

        System.out.println("Building task list");
        SmartQ<DefaultTaskResult> queue = makeQueue();
        List<Task> tasks = new LinkedList();
        final int factor = 100;

        int[] sizes = new int[]{22,53,11,5,82};
        int totalSize = 0;

        for(int size : sizes) {
            size *= factor;
            totalSize += size;
            for(int i = 0; i < size; i++) {
                String type = "rate" + (1 + (i % 5));
                Task t = new Task(type);
                t.addTag(Math.random() > .5 ? "web1" : "web2");
                t.setPriority((int) Math.round(Math.random() * 5));
                tasks.add(t);
            }
        }

        System.out.println("Setting up queue");
        long beforeSubmit = System.currentTimeMillis();
        queue.submit(tasks);
        long submitTook = System.currentTimeMillis() - beforeSubmit;
        System.out.println(String.format("Spent %s ms on inserting %s tasks",submitTook, totalSize));
        if (queue.getStore() instanceof PostgresTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 2300ms", submitTook < 2800L);
        } else if (queue.getStore() instanceof MemoryTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 200ms", submitTook < 400L);
        } else if (queue.getStore() instanceof WriteThroughTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 200ms", submitTook < 400L);
        } else {
            fail("Unknown task store");
        }

        queue.setRateLimit("rate1", 10);
        queue.setRateLimit("rate2", 5);
        queue.setRateLimit("rate3", 7);
        queue.setRateLimit("rate4", 10);
        queue.setRateLimit("rate5", 15);

        queue.setRateLimit("web1", 40);
        queue.setRateLimit("web2", 40);

        queue.setEstimateForTaskType("rate1", 3113L);
        queue.setEstimateForTaskType("rate2", 1331L);
        queue.setEstimateForTaskType("rate3", 23210L);
        queue.setEstimateForTaskType("rate4", 321L);
        queue.setEstimateForTaskType("rate5", 5122L);
        queue.setSubscribers(60);

        System.out.println("Getting task list");
        ParallelIterator<Task> pending = queue.getStore().getPending();

        QueueEstimator estimator = new QueueEstimator(queue);
        //estimator.setSpeed(QueueEstimator.Speed.FASTEST);

        System.out.println("Starting ETA calculation");
        long start = System.currentTimeMillis();
        long eta = estimator.queueEnds(pending);
        long timeTaken = System.currentTimeMillis() - start;

        System.out.println(String.format("ETA %s ms, Calculation time:  %s ms, Q Size: %s, Free mem: %s",
                eta, timeTaken, totalSize, Runtime.getRuntime().freeMemory()));

        assertEquals(11488950L , eta );
        assertEquals(17300 , totalSize);

        if (queue.getStore() instanceof PostgresTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 2500ms", timeTaken < 2500L);
        } else if (queue.getStore() instanceof MemoryTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 500ms", timeTaken < 500L);
        } else if (queue.getStore() instanceof WriteThroughTaskStore) {
            assertTrue("For a factor = 100 this should be around less than 500ms", timeTaken < 500L);
        } else {
            fail("Unknown task store");
        }
    }

    private static class ThreadedWaiter extends Thread {
        private final TaskStore store;
        private boolean done;

        private ThreadedWaiter(TaskStore store) {
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
        private final SmartQ<DefaultTaskResult> queue;
        private boolean done = false;
        private Task task;

        private ThreadedSubscriber(SmartQ<DefaultTaskResult> queue, String name) {
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
    
    private void assertHashEquals(Object expected, Object actual) {
        if (expected.hashCode() != actual.hashCode()) {
            fail(Assert.format(null, expected, actual));
        }
    }

    private static class ThreadedRunner extends Thread {
        private final SmartQ<DefaultTaskResult> queue;
        private boolean done;

        private ThreadedRunner(SmartQ<DefaultTaskResult> queue) {
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
