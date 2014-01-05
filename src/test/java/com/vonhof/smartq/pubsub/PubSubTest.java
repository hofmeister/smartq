package com.vonhof.smartq.pubsub;


import com.vonhof.smartq.DefaultTaskResult;
import com.vonhof.smartq.MemoryTaskStore;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.SocketProxy;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.Task.State;
import com.vonhof.smartq.TaskStore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PubSubTest {

    protected synchronized InetSocketAddress generateAddress() {
        int port = 50000 + (int)Math.round(Math.random()*5000);
        return new InetSocketAddress("127.0.0.1",port);
    }

    protected synchronized TaskStore<Task> makeStore() {
        return new MemoryTaskStore<Task>();
    }

    protected synchronized SmartQ<Task,DefaultTaskResult> makeQueue() {
        return new SmartQ<Task, DefaultTaskResult>(makeStore());
    }

    protected synchronized SocketProxy makeProxy() throws Exception {
        return new SocketProxy(generateAddress(), generateAddress());
    }

    protected synchronized SmartQPublisher<Task> makeProducer() {
        return makeProducer(generateAddress());
    }

    protected synchronized SmartQPublisher<Task> makeProducer(InetSocketAddress address) {
        return new SmartQPublisher<Task>(address,makeQueue());
    }



    @Test
    public void can_acquire_over_the_wire() throws IOException, InterruptedException {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappySubscriberHandler subscriberHandler = new HappySubscriberHandler();
        final SmartQSubscriber<Task> subscriber = producer.makeSubscriber(subscriberHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (subscriberHandler) {
            if (!subscriberHandler.done) {
                subscriberHandler.wait(1000);
            }
        }

        assertTrue(subscriberHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        subscriber.acknowledge(task.getId());

        Thread.sleep(100);

        assertEquals(0, queue.size());

        subscriber.close();
        producer.close();

    }

    @Test
    public void can_cancel_over_the_wire() throws IOException, InterruptedException {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappySubscriberHandler subscriberHandler = new HappySubscriberHandler();
        final SmartQSubscriber<Task> subscriber = producer.makeSubscriber(subscriberHandler);

        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);
        queue.submit(task2);

        producer.listen();

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(2, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals("We have 1 subscriber", 1, producer.getSubscriberCount());

        assertEquals(1, queue.queueSize());
        assertEquals(1, queue.runningCount());


        synchronized (subscriberHandler) {
            if (!subscriberHandler.done) {
                subscriberHandler.wait(1000);
            }
        }

        assertTrue("Subscriber handler was executed successfully", subscriberHandler.done);
        assertEquals("We have 1 task in queue",1, queue.queueSize());
        assertEquals("We have 1 running task", 1, queue.runningCount());

        //Cancel the task without rescheduling it
        subscriberHandler.done = false;
        subscriber.cancel(subscriberHandler.task.getId(), false);

        Thread.sleep(100);

        assertEquals("We have 1 task left in the queue", 1, queue.size());

        //Grab the next task

        synchronized (subscriberHandler) {
            if (!subscriberHandler.done) {
                subscriberHandler.wait(1000);
            }
        }

        assertTrue(subscriberHandler.done);
        assertEquals("All tasks have been acquired",0, queue.queueSize());
        assertEquals("We have a running task",1, queue.runningCount());

        //Cancel the task and reschedule
        subscriber.cancel(subscriberHandler.task.getId(), true);

        Thread.sleep(100);

        assertEquals("Queue should still have 1 since we rescheduled",1, queue.size());

        subscriber.close();
        producer.close();
    }


    @Test
    public void client_disconnect_causes_auto_retry() throws IOException, InterruptedException {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappySubscriberHandler subscriberHandler = new HappySubscriberHandler();
        final SmartQSubscriber<Task> subscriber = producer.makeSubscriber(subscriberHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (subscriberHandler) {
            subscriberHandler.wait(1000);
        }

        assertTrue(subscriberHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        subscriber.close();

        Thread.sleep(500);

        assertEquals(0, producer.getSubscriberCount());

        assertEquals(0, queue.runningCount());

        assertEquals("Subscriber went away, the task has been rescheduled", 1, queue.size());

        producer.close();
    }

    @Test
    public void client_unhandled_exception_causes_auto_cancel() throws IOException, InterruptedException {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final ExceptionSubscriberHandler subscriberHandler = new ExceptionSubscriberHandler();
        final SmartQSubscriber<Task> subscriber = producer.makeSubscriber(subscriberHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        Thread.sleep(100);

        assertEquals("The task is in error state", State.ERROR, queue.getStore().get(task.getId()).getState());
        assertEquals("The task was not auto-rescheduled", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());

        subscriber.close();
        producer.close();
    }

    @Test
    public void client_unhandled_exception_causes_retry_if_allowed() throws IOException, InterruptedException {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final ControllableExceptionSubscriberHandler subscriberHandler = new ControllableExceptionSubscriberHandler();
        final SmartQSubscriber<Task> subscriber = producer.makeSubscriber(subscriberHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());
        queue.setMaxRetries("test", 2);

        assertEquals("Task has 0 attempts", 0, task.getAttempts());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        subscriberHandler.wakeUp();
        Thread.sleep(100);
        subscriberHandler.setThrowing(false);
        Thread.sleep(200);


        Task refreshedTask = queue.getStore().get(task.getId());

        assertEquals("Task is in running state", State.RUNNING, refreshedTask.getState());
        assertEquals("Task has 1 attempt", 1, refreshedTask.getAttempts());
        assertEquals("The task is running again", 1, queue.runningCount());

        subscriberHandler.wakeUp();
        Thread.sleep(100);


        assertTrue("The task is done", subscriberHandler.isDone());
        subscriber.acknowledge(task.getId());
        Thread.sleep(100);

        assertNull("The task is removed from the store", queue.getStore().get(task.getId()));
        assertEquals("The task was no longer in queue", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());


        subscriber.close();
        producer.close();
    }

    @Test
     public void when_server_goes_away_it_moves_running_to_queued_when_restarted() throws Exception {
        final SocketProxy proxy = makeProxy();
        final SmartQPublisher<Task> producer = makeProducer(proxy.getTarget());

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappySubscriberHandler subscriberHandler = new HappySubscriberHandler();
        final SmartQSubscriber<Task> subscriber = new SmartQSubscriber<Task>(proxy.getAddress(), subscriberHandler);

        subscriber.setRetryTimeout(100);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        Thread.sleep(100);

        assertEquals(0, producer.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        subscriber.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        proxy.close();
        producer.close();

        Thread.sleep(500);

        assertEquals("Clients was disconnected", 0, producer.getSubscriberCount());

        subscriber.acknowledge(task.getId()); //Client acks when offline

        assertEquals("Queue size is not affected",1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        SmartQPublisher<Task> newProducer = new SmartQPublisher<Task>(proxy.getTarget(), queue);

        newProducer.listen();
        proxy.reopen();

        Thread.sleep(1500);

        assertEquals("Client auto reconnects", 1, producer.getSubscriberCount());

        Thread.sleep(200);

        assertEquals(0, queue.queueSize());
        assertEquals("Running task has been ack'ed when client reconnected",0, queue.runningCount());

        subscriber.close();
        newProducer.close();
        proxy.close();
    }


    @Test
    public void multiple_subscribers_are_supported() throws Exception {
        final SmartQPublisher<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final MultiSubscriberHandler subscriberHandler = new MultiSubscriberHandler();
        final SmartQSubscriber<Task> subscriber1 = producer.makeSubscriber(subscriberHandler);
        final SmartQSubscriber<Task> subscriber2 = producer.makeSubscriber(subscriberHandler);
        final SmartQSubscriber<Task> subscriber3 = producer.makeSubscriber(subscriberHandler);


        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());
        final Task task3 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);

        producer.listen();

        Thread.sleep(100);

        assertEquals(0, producer.getSubscriberCount());

        subscriber1.connect();
        subscriber2.connect();
        subscriber3.connect();

        Thread.sleep(100);

        assertEquals(3, producer.getSubscriberCount());

        Thread.sleep(100);

        assertTrue(subscriberHandler.taskIds.contains(task1.getId()));

        subscriberHandler.subscriberMap.get(task1.getId()).acknowledge(task1.getId());

        queue.submit(task2);
        queue.submit(task3);

        Thread.sleep(1500);

        assertEquals(0, queue.queueSize());
        assertEquals(2, queue.runningCount());

        subscriber1.close();
        subscriber2.close();
        subscriber3.close();
        producer.close();
    }


    public static class MultiSubscriberHandler implements SmartQSubscriberHandler<Task> {

        private int done = 0;
        private List<UUID> taskIds = new ArrayList<UUID>();
        private Map<UUID,SmartQSubscriber<Task>> subscriberMap = new HashMap<UUID, SmartQSubscriber<Task>>();

        @Override
        public void taskReceived(SmartQSubscriber<Task> subscriber, Task task) {
            try {
                subscriberMap.put(task.getId(), subscriber);
                taskIds.add(task.getId());
                Thread.sleep(100); //Do some work
                done++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

    public static class HappySubscriberHandler implements SmartQSubscriberHandler<Task> {

        private boolean done = false;
        private boolean failed = false;
        private Task task;

        @Override
        public void taskReceived(SmartQSubscriber<Task> subscriber, Task task) {
            try {
                this.task = task;
                Thread.sleep(100); //Do some work

                done = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (!done) {
                    failed = true;
                }
                synchronized (this) {
                    notifyAll();
                }
            }
        }
    }

    public static class ExceptionSubscriberHandler implements SmartQSubscriberHandler<Task> {

        private Task task;

        @Override
        public void taskReceived(SmartQSubscriber<Task> subscriber, Task task) throws Exception {
            this.task = task;
            Thread.sleep(100); //Do some work
            throw new Exception("Something went wrong");
        }
    }

    public static class ControllableExceptionSubscriberHandler implements SmartQSubscriberHandler<Task> {

        private Task task;

        private boolean throwing = true;
        private boolean done = false;

        public boolean isThrowing() {
            return throwing;
        }

        public void setThrowing(boolean throwing) {
            this.throwing = throwing;
        }

        public boolean isDone() {
            return done;
        }

        public synchronized void wakeUp() {
            notifyAll();
        }

        @Override
        public void taskReceived(SmartQSubscriber<Task> subscriber, Task task) throws Exception {
            this.task = task;
            System.out.println("Waiting");
            synchronized (this) {
                wait();
            }
            System.out.println("Woke up");
            if (throwing) {
                throw new Exception("Something went wrong");
            }
            System.out.println("done");
            done = true;
        }
    }
}
