package com.vonhof.smartq.server;


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

public class ConsumerProducerTest {

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

    protected synchronized SmartQProducer<Task> makeProducer() {
        return makeProducer(generateAddress());
    }

    protected synchronized SmartQProducer<Task> makeProducer(InetSocketAddress address) {
        return new SmartQProducer<Task>(address,makeQueue());
    }



    @Test
    public void can_acquire_over_the_wire() throws IOException, InterruptedException {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappyConsumerHandler consumerHandler = new HappyConsumerHandler();
        final SmartQConsumer<Task> consumer = producer.makeConsumer(consumerHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getConsumerCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getConsumerCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (consumerHandler) {
            if (!consumerHandler.done) {
                consumerHandler.wait(1000);
            }
        }

        assertTrue(consumerHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        consumer.acknowledge(task.getId());

        Thread.sleep(100);

        assertEquals(0, queue.size());

        consumer.close();
        producer.close();

    }

    @Test
    public void can_cancel_over_the_wire() throws IOException, InterruptedException {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappyConsumerHandler consumerHandler = new HappyConsumerHandler();
        final SmartQConsumer<Task> consumer = producer.makeConsumer(consumerHandler);

        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);
        queue.submit(task2);

        producer.listen();

        assertEquals(0, producer.getConsumerCount());
        assertEquals(2, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals("We have 1 consumer", 1, producer.getConsumerCount());

        assertEquals(1, queue.queueSize());
        assertEquals(1, queue.runningCount());


        synchronized (consumerHandler) {
            if (!consumerHandler.done) {
                consumerHandler.wait(1000);
            }
        }

        assertTrue("Consumer handler was executed successfully", consumerHandler.done);
        assertEquals("We have 1 task in queue",1, queue.queueSize());
        assertEquals("We have 1 running task", 1, queue.runningCount());

        //Cancel the task without rescheduling it
        consumerHandler.done = false;
        consumer.cancel(consumerHandler.task.getId(), false);

        Thread.sleep(100);

        assertEquals("We have 1 task left in the queue", 1, queue.size());

        //Grab the next task

        synchronized (consumerHandler) {
            if (!consumerHandler.done) {
                consumerHandler.wait(1000);
            }
        }

        assertTrue(consumerHandler.done);
        assertEquals("All tasks have been acquired",0, queue.queueSize());
        assertEquals("We have a running task",1, queue.runningCount());

        //Cancel the task and reschedule
        consumer.cancel(consumerHandler.task.getId(), true);

        Thread.sleep(100);

        assertEquals("Queue should still have 1 since we rescheduled",1, queue.size());

        consumer.close();
        producer.close();
    }


    @Test
    public void client_disconnect_causes_auto_retry() throws IOException, InterruptedException {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappyConsumerHandler consumerHandler = new HappyConsumerHandler();
        final SmartQConsumer<Task> consumer = producer.makeConsumer(consumerHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getConsumerCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getConsumerCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (consumerHandler) {
            consumerHandler.wait(1000);
        }

        assertTrue(consumerHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        consumer.close();

        Thread.sleep(500);

        assertEquals(0, producer.getConsumerCount());

        assertEquals(0, queue.runningCount());

        assertEquals("Consumer went away, the task has been rescheduled", 1, queue.size());

        producer.close();
    }

    @Test
    public void client_unhandled_exception_causes_auto_cancel() throws IOException, InterruptedException {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final ExceptionConsumerHandler consumerHandler = new ExceptionConsumerHandler();
        final SmartQConsumer<Task> consumer = producer.makeConsumer(consumerHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getConsumerCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getConsumerCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        Thread.sleep(100);

        assertEquals("The task is in error state", State.ERROR, queue.getStore().get(task.getId()).getState());
        assertEquals("The task was not auto-rescheduled", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());

        consumer.close();
        producer.close();
    }

    @Test
    public void client_unhandled_exception_causes_retry_if_allowed() throws IOException, InterruptedException {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final ControllableExceptionConsumerHandler consumerHandler = new ControllableExceptionConsumerHandler();
        final SmartQConsumer<Task> consumer = producer.makeConsumer(consumerHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());
        queue.setMaxRetries("test", 2);

        assertEquals("Task has 0 attempts", 0, task.getAttempts());

        queue.submit(task);

        producer.listen();

        assertEquals(0, producer.getConsumerCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getConsumerCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        consumerHandler.wakeUp();
        Thread.sleep(100);
        consumerHandler.setThrowing(false);
        Thread.sleep(200);


        Task refreshedTask = queue.getStore().get(task.getId());

        assertEquals("Task is in running state", State.RUNNING, refreshedTask.getState());
        assertEquals("Task has 1 attempt", 1, refreshedTask.getAttempts());
        assertEquals("The task is running again", 1, queue.runningCount());

        consumerHandler.wakeUp();
        Thread.sleep(100);


        assertTrue("The task is done", consumerHandler.isDone());
        consumer.acknowledge(task.getId());
        Thread.sleep(100);

        assertNull("The task is removed from the store", queue.getStore().get(task.getId()));
        assertEquals("The task was no longer in queue", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());


        consumer.close();
        producer.close();
    }

    @Test
     public void when_server_goes_away_it_moves_running_to_queued_when_restarted() throws Exception {
        final SocketProxy proxy = makeProxy();
        final SmartQProducer<Task> producer = makeProducer(proxy.getTarget());

        final SmartQ<Task, ?> queue = producer.getQueue();

        final HappyConsumerHandler consumerHandler = new HappyConsumerHandler();
        final SmartQConsumer<Task> consumer = new SmartQConsumer<Task>(proxy.getAddress(), consumerHandler);

        consumer.setRetryTimeout(100);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        producer.listen();

        Thread.sleep(100);

        assertEquals(0, producer.getConsumerCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        consumer.connect();

        Thread.sleep(100);

        assertEquals(1, producer.getConsumerCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        proxy.close();
        producer.close();

        Thread.sleep(500);

        assertEquals("Clients was disconnected", 0, producer.getConsumerCount());

        consumer.acknowledge(task.getId()); //Client acks when offline

        assertEquals("Queue size is not affected",1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        SmartQProducer<Task> newProducer = new SmartQProducer<Task>(proxy.getTarget(), queue);

        newProducer.listen();
        proxy.reopen();

        Thread.sleep(1500);

        assertEquals("Client auto reconnects", 1, producer.getConsumerCount());

        Thread.sleep(200);

        assertEquals(0, queue.queueSize());
        assertEquals("Running task has been ack'ed when client reconnected",0, queue.runningCount());

        consumer.close();
        newProducer.close();
        proxy.close();
    }


    @Test
    public void multiple_consumers_are_supported() throws Exception {
        final SmartQProducer<Task> producer = makeProducer();

        final SmartQ<Task, ?> queue = producer.getQueue();

        final MultiConsumerHandler consumerHandler = new MultiConsumerHandler();
        final SmartQConsumer<Task> consumer1 = producer.makeConsumer(consumerHandler);
        final SmartQConsumer<Task> consumer2 = producer.makeConsumer(consumerHandler);
        final SmartQConsumer<Task> consumer3 = producer.makeConsumer(consumerHandler);


        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());
        final Task task3 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);

        producer.listen();

        Thread.sleep(100);

        assertEquals(0, producer.getConsumerCount());

        consumer1.connect();
        consumer2.connect();
        consumer3.connect();

        Thread.sleep(100);

        assertEquals(3, producer.getConsumerCount());

        Thread.sleep(100);

        assertTrue(consumerHandler.taskIds.contains(task1.getId()));

        consumerHandler.consumerMap.get(task1.getId()).acknowledge(task1.getId());

        queue.submit(task2);
        queue.submit(task3);

        Thread.sleep(1500);

        assertEquals(0, queue.queueSize());
        assertEquals(2, queue.runningCount());

        consumer1.close();
        consumer2.close();
        consumer3.close();
        producer.close();
    }


    public static class MultiConsumerHandler implements SmartQConsumerHandler<Task> {

        private int done = 0;
        private List<UUID> taskIds = new ArrayList<UUID>();
        private Map<UUID,SmartQConsumer<Task>> consumerMap = new HashMap<UUID, SmartQConsumer<Task>>();

        @Override
        public void taskReceived(SmartQConsumer<Task> consumer, Task task) {
            try {
                consumerMap.put(task.getId(),consumer);
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

    public static class HappyConsumerHandler implements SmartQConsumerHandler<Task> {

        private boolean done = false;
        private boolean failed = false;
        private Task task;

        @Override
        public void taskReceived(SmartQConsumer<Task> consumer, Task task) {
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

    public static class ExceptionConsumerHandler implements SmartQConsumerHandler<Task> {

        private Task task;

        @Override
        public void taskReceived(SmartQConsumer<Task> consumer, Task task) throws Exception {
            this.task = task;
            Thread.sleep(100); //Do some work
            throw new Exception("Something went wrong");
        }
    }

    public static class ControllableExceptionConsumerHandler implements SmartQConsumerHandler<Task> {

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
        public void taskReceived(SmartQConsumer<Task> consumer, Task task) throws Exception {
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
