package com.vonhof.smartq.server;


import com.vonhof.smartq.DefaultTaskResult;
import com.vonhof.smartq.MemoryTaskStore;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.SocketProxy;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.Task.State;
import com.vonhof.smartq.TaskStore;
import org.junit.After;
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

public class ClientServerTest {

    protected synchronized InetSocketAddress generateAddress() {
        int port = 50000 + (int)Math.round(Math.random()*5000);
        return new InetSocketAddress("127.0.0.1",port);
    }

    protected synchronized TaskStore makeStore() {
        return new MemoryTaskStore();
    }

    protected synchronized SmartQ<DefaultTaskResult> makeQueue() {
        return new SmartQ<DefaultTaskResult>(makeStore());
    }

    protected synchronized SocketProxy makeProxy() throws Exception {
        return new SocketProxy(generateAddress(), generateAddress());
    }

    protected synchronized SmartQServer makeServer() {
        return makeServer(generateAddress());
    }

    protected synchronized SmartQServer makeServer(InetSocketAddress address) {
        return new SmartQServer(address,makeQueue());
    }




    @Test
    public void can_acquire_over_the_wire() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        server.listen();

        assertEquals(0, server.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (msgHandler) {
            if (!msgHandler.done) {
                msgHandler.wait(1000);
            }
        }

        assertTrue(msgHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        client.acknowledge(task.getId());

        Thread.sleep(100);

        assertEquals(0, queue.size());

        client.close();
        server.close();

    }

    @Test
    public void client_supports_auto_acknowledge() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);
        client.setAutoAcknowledge(true);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        server.listen();
        client.connect();

        synchronized (msgHandler) {
            if (!msgHandler.done) {
                msgHandler.wait(1000);
            }
        }

        assertTrue(msgHandler.done);

        Thread.sleep(100);

        assertEquals(0, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.close();
        server.close();

    }

    @Test
    public void can_cancel_over_the_wire() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);

        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);
        queue.submit(task2);

        server.listen();

        assertEquals(0, server.getSubscriberCount());
        assertEquals(2, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals("We have 1 client", 1, server.getSubscriberCount());

        assertEquals(1, queue.queueSize());
        assertEquals(1, queue.runningCount());


        synchronized (msgHandler) {
            if (!msgHandler.done) {
                msgHandler.wait(1000);
            }
        }

        assertTrue("Subscriber handler was executed successfully", msgHandler.done);
        assertEquals("We have 1 task in queue",1, queue.queueSize());
        assertEquals("We have 1 running task", 1, queue.runningCount());

        //Cancel the task without rescheduling it
        msgHandler.done = false;
        client.cancel(msgHandler.task.getId(), false);

        Thread.sleep(100);

        assertEquals("We have 1 task left in the queue", 1, queue.size());

        //Grab the next task

        synchronized (msgHandler) {
            if (!msgHandler.done) {
                msgHandler.wait(1000);
            }
        }

        assertTrue(msgHandler.done);
        assertEquals("All tasks have been acquired",0, queue.queueSize());
        assertEquals("We have a running task",1, queue.runningCount());

        //Cancel the task and reschedule
        client.cancel(msgHandler.task.getId(), true);

        Thread.sleep(100);

        assertEquals("Queue should still have 1 since we rescheduled",1, queue.size());

        client.close();
        server.close();
    }


    @Test
    public void client_disconnect_causes_auto_retry() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        server.listen();

        assertEquals(0, server.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        synchronized (msgHandler) {
            msgHandler.wait(1000);
        }

        assertTrue(msgHandler.done);
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        client.close();

        Thread.sleep(500);

        assertEquals(0, server.getSubscriberCount());

        assertEquals(0, queue.runningCount());

        assertEquals("Subscriber went away, the task has been rescheduled", 1, queue.size());

        server.close();
    }

    @Test
    public void client_unhandled_exception_causes_auto_cancel() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final ExceptionClientMessageHandler msgHandler = new ExceptionClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        server.listen();

        assertEquals(0, server.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        Thread.sleep(100);

        assertEquals("The task is in error state", State.ERROR, queue.getStore().get(task.getId()).getState());
        assertEquals("The task was not auto-rescheduled", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());

        client.close();
        server.close();
    }

    @Test
    public void client_unhandled_exception_causes_retry_if_allowed() throws IOException, InterruptedException {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final ControllableExceptionClientMessageHandler msgHandler = new ControllableExceptionClientMessageHandler();
        final SmartQClient client = server.makeClient(msgHandler);

        final Task task = new Task("test").withId(UUID.randomUUID());
        queue.setMaxRetries("test", 2);

        assertEquals("Task has 0 attempts", 0, task.getAttempts());

        queue.submit(task);

        server.listen();

        assertEquals(0, server.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        msgHandler.wakeUp();
        Thread.sleep(100);
        msgHandler.setThrowing(false);
        Thread.sleep(200);


        Task refreshedTask = queue.getStore().get(task.getId());

        assertEquals("Task is in running state", State.RUNNING, refreshedTask.getState());
        assertEquals("Task has 1 attempt", 1, refreshedTask.getAttempts());
        assertEquals("The task is running again", 1, queue.runningCount());

        msgHandler.wakeUp();
        Thread.sleep(100);


        assertTrue("The task is done", msgHandler.isDone());
        client.acknowledge(task.getId());
        Thread.sleep(100);

        assertNull("The task is removed from the store", queue.getStore().get(task.getId()));
        assertEquals("The task was no longer in queue", 0, queue.queueSize());
        assertEquals("The task is no longer running", 0, queue.runningCount());


        client.close();
        server.close();
    }

    @Test
     public void when_server_goes_away_it_moves_running_to_queued_when_restarted() throws Exception {
        final SocketProxy proxy = makeProxy();
        final SmartQServer server = makeServer(proxy.getTarget());

        final SmartQ<?> queue = server.getQueue();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();
        final SmartQClient client = new SmartQClient(proxy.getAddress(), msgHandler, 1);

        client.setRetryTimeout(100);

        final Task task = new Task("test").withId(UUID.randomUUID());

        queue.submit(task);

        server.listen();

        Thread.sleep(100);

        assertEquals(0, server.getSubscriberCount());
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        client.connect();

        Thread.sleep(100);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.runningCount());

        proxy.close();
        server.close();

        Thread.sleep(500);

        assertEquals("Clients was disconnected", 0, server.getSubscriberCount());

        client.acknowledge(task.getId()); //Client acks when offline

        assertEquals("Queue size is not affected", 1, queue.queueSize());
        assertEquals(0, queue.runningCount());

        SmartQServer newServer = new SmartQServer(proxy.getTarget(), queue);

        newServer.listen();
        proxy.reopen();

        Thread.sleep(500);

        assertEquals("Client auto reconnects", 1, newServer.getClientCount());
        assertEquals("Client auto reconnects", 1, newServer.getSubscriberCount());

        Thread.sleep(200);

        assertEquals(0, queue.queueSize());
        assertEquals("Running task has been ack'ed when client reconnected",0, queue.runningCount());

        client.close();
        newServer.close();
        proxy.close();
    }


    @Test
    public void multiple_clients_are_supported() throws Exception {
        final SmartQServer server = makeServer();

        final SmartQ<?> queue = server.getQueue();

        final MultiClientMessageHandler msgHandler = new MultiClientMessageHandler();
        final SmartQClient client1 = server.makeClient(msgHandler);
        final SmartQClient client2 = server.makeClient(msgHandler);
        final SmartQClient client3 = server.makeClient(msgHandler);


        final Task task1 = new Task("test").withId(UUID.randomUUID());
        final Task task2 = new Task("test").withId(UUID.randomUUID());
        final Task task3 = new Task("test").withId(UUID.randomUUID());

        queue.submit(task1);

        server.listen();

        Thread.sleep(100);

        assertEquals(0, server.getSubscriberCount());

        client1.connect();
        client2.connect();
        client3.connect();

        Thread.sleep(100);

        assertEquals(3, server.getSubscriberCount());

        Thread.sleep(100);

        assertTrue(msgHandler.taskIds.contains(task1.getId()));

        msgHandler.clientMap.get(task1.getId()).acknowledge(task1.getId());

        queue.submit(task2);
        queue.submit(task3);

        Thread.sleep(1500);

        assertEquals(0, queue.queueSize());
        assertEquals(2, queue.runningCount());

        client1.close();
        client2.close();
        client3.close();
        server.close();
    }

    @Test
    public void clients_can_publish_tasks() throws Exception {
        final SmartQServer server = makeServer();

        final ControllableExceptionClientMessageHandler msgHandler = new ControllableExceptionClientMessageHandler();
        msgHandler.setThrowing(false);

        final SmartQClient clientPublisher = server.makeClient();
        final SmartQClient clientSubscriber = server.makeClient(msgHandler);

        final Task task1 = new Task("test").withId(UUID.randomUUID());

        server.listen();

        Thread.sleep(100);

        assertEquals(0, server.getSubscriberCount());
        assertEquals(0, server.getClientCount());

        clientPublisher.connect();

        Thread.sleep(100);

        assertEquals("Publish-only clients do not count as subscriber", 0, server.getSubscriberCount());
        assertEquals(1, server.getClientCount());

        Thread.sleep(100);

        clientPublisher.publish(task1);

        Thread.sleep(100);

        assertEquals(1, server.getQueue().queueSize());

        clientSubscriber.connect();

        Thread.sleep(200);

        assertEquals(1, server.getSubscriberCount());
        assertEquals(1, server.getQueue().runningCount());
        assertEquals(2, server.getClientCount());

        msgHandler.wakeUp();

        Thread.sleep(200);

        assertTrue(msgHandler.isDone());
        clientSubscriber.acknowledge(task1.getId());

        Thread.sleep(200);

        assertEquals(0, server.getQueue().queueSize());
        assertEquals(0, server.getQueue().runningCount());

        clientPublisher.close();
        clientSubscriber.close();
        server.close();
    }

    @Test
    public void large_tasks_are_supported() throws Exception {
        final SmartQServer server = makeServer();

        final HappyClientMessageHandler msgHandler = new HappyClientMessageHandler();

        final SmartQClient clientPublisher = server.makeClient();
        final SmartQClient clientSubscriber = server.makeClient(msgHandler);

        final Task task1 = new Task("test").withId(UUID.randomUUID());
        task1.setData(new byte[25000]);

        server.listen();

        Thread.sleep(100);

        assertEquals(0, server.getSubscriberCount());
        assertEquals(0, server.getClientCount());

        clientPublisher.connect();

        Thread.sleep(100);

        clientPublisher.publish(task1);

        Thread.sleep(100);

        assertEquals(1, server.getQueue().queueSize());

        clientSubscriber.connect();

        Thread.sleep(200);

        assertTrue(msgHandler.done);

        clientSubscriber.acknowledge(task1.getId());

        clientPublisher.close();
        clientSubscriber.close();
        server.close();
    }


    public static class MultiClientMessageHandler implements SmartQClientMessageHandler {

        private int done = 0;
        private List<UUID> taskIds = new ArrayList<UUID>();
        private Map<UUID,SmartQClient> clientMap = new HashMap<UUID, SmartQClient>();

        @Override
        public void taskReceived(SmartQClient client, Task task) {
            try {
                clientMap.put(task.getId(), client);
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

    public static class HappyClientMessageHandler implements SmartQClientMessageHandler {

        private boolean done = false;
        private boolean failed = false;
        private Task task;

        @Override
        public void taskReceived(SmartQClient client, Task task) {
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

    public static class ExceptionClientMessageHandler implements SmartQClientMessageHandler {

        private Task task;

        @Override
        public void taskReceived(SmartQClient client, Task task) throws Exception {
            this.task = task;
            Thread.sleep(100); //Do some work
            throw new Exception("Something went wrong");
        }
    }

    public static class ControllableExceptionClientMessageHandler implements SmartQClientMessageHandler {

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
        public void taskReceived(SmartQClient client, Task task) throws Exception {
            this.task = task;
            synchronized (this) {
                wait();
            }
            if (throwing) {
                throw new Exception("Something went wrong");
            }
            done = true;
        }
    }
}
