package com.vonhof.smartq.pubsub;


import com.vonhof.smartq.AcquireInterruptedException;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.util.ConcurrentHashSet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartQPublisher<T extends Task> {

    private static final Logger log = Logger.getLogger(SmartQPublisher.class);

    private final InetSocketAddress address;
    private final SmartQ<T,?> queue;

    private NioSocketAcceptor acceptor;
    private final AtomicInteger subscribers = new AtomicInteger(0);
    private final RequestHandler requestHandler = new RequestHandler();
    private TaskEmitter taskEmitter;
    private final Timer timer = new Timer();


    public SmartQPublisher(InetSocketAddress address, SmartQ queue) {
        this.address = address;
        this.queue = queue;

    }

    public int getSubscriberCount() {
        return queue.getSubscribers();
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public SmartQ<T,?> getQueue() {
        return queue;
    }

    public SmartQSubscriber<T> makeSubscriber(SmartQSubscriberHandler<T> handler) {
        return new SmartQSubscriber<T>(address, handler);
    }

    public synchronized void listen() throws IOException {

        acceptor = new NioSocketAcceptor();
        acceptor.setReuseAddress(true);

        ObjectSerializationCodecFactory codecFactory = new ObjectSerializationCodecFactory();
        codecFactory.setDecoderMaxObjectSize(Integer.MAX_VALUE);
        codecFactory.setEncoderMaxObjectSize(Integer.MAX_VALUE);

        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( codecFactory ));

        acceptor.setHandler( requestHandler );


        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.setCloseOnDeactivation(true);
        acceptor.bind( address );

        log.debug(String.format("Listening on " + address));

        taskEmitter = new TaskEmitter();
        taskEmitter.start();

        timer.scheduleAtFixedRate(new StaleTaskMonitor(),60000,300000);

    }

    public synchronized void close()  {
        acceptor.unbind();
        acceptor.dispose(true);
        acceptor = null;

        log.debug(String.format("Stopped listening on " + address));
        queue.interrupt();

        if (taskEmitter != null) {
            taskEmitter.interrupt();

            try {
                taskEmitter.join();
            } catch (InterruptedException e) {

            }
            taskEmitter = null;
        }

        timer.cancel();
        timer.purge();

    }


    private class RequestHandler extends IoHandlerAdapter {
        private final Map<SocketAddress,List<UUID>> clientTask = new ConcurrentHashMap<SocketAddress, List<UUID>>();
        private final Set<Long> sessionReady = new ConcurrentHashSet<Long>();
        private final Set<UUID> taskIds = new ConcurrentHashSet<UUID>();
        private int taskLimit = 1;

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
            log.error("Got exception while handling request", cause);
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            if (message instanceof Command) {
                handleCommand(session, (Command) message);
                return;
            }

            session.write(new Error("Invalid message: " + message));
        }

        public boolean taskIsRunning(UUID id) {
            return taskIds.contains(id);
        }

        public void registerTask(IoSession session, UUID id) {
            clientTask.get(session.getRemoteAddress()).add(id);
            taskIds.add(id);

            synchronized (clientTask) {
                clientTask.notifyAll();
            }
        }

        public void unregisterTask(IoSession session, UUID id) {
            clientTask.get(session.getRemoteAddress()).remove(id);
            taskIds.remove(id);

            synchronized (clientTask) {
                clientTask.notifyAll();
            }
        }

        public void unregisterTask(UUID id) {
            for(Entry<SocketAddress, List<UUID>> entry : clientTask.entrySet()) {
                if (entry.getValue().contains(id)) {
                    entry.getValue().remove(id);
                    break;
                }
            }

            taskIds.remove(id);

            synchronized (clientTask) {
                clientTask.notifyAll();
            }
        }

        public boolean isReady(IoSession session) {
            return sessionReady.contains(session.getId());
        }

        public boolean isAlive(IoSession session) {
            return session != null
                    && session.isConnected()
                    && clientTask.containsKey(session.getRemoteAddress());
        }

        public boolean isBusy(IoSession session) {
            if (!isAlive(session)) {
                return true;
            }

            List<UUID> tasks = clientTask.get(session.getRemoteAddress());
            return tasks == null || (tasks.size() >= taskLimit);
        }

        public void sendTask(IoSession session, T task) throws InterruptedException {
            if (!isAlive(session)) {
                return;
            }
            while(isAlive(session) && isBusy(session)) {
                if (clientTask.get(session.getRemoteAddress()).contains(task.getId())) {
                    log.debug("Waiting for client to tell us what it knows: " + session.getRemoteAddress());
                    break; //We are resending something
                }

                synchronized (clientTask) {
                    clientTask.wait(5000);
                }

                log.debug("Waiting for client to become available: " + session.getRemoteAddress());
            }
            if (!isAlive(session)) {
                return;
            }

            log.debug("Sending task to client: " + session.getRemoteAddress() + " - " + task.getId());
            registerTask(session, task.getId());
            session.write(task);
        }

        public void endTask(IoSession session, UUID taskId) {
            if (!isAlive(session)) {
                return;
            }
            unregisterTask(session, taskId);

            synchronized (taskEmitter) {
                taskEmitter.notifyAll();
            }
        }

        private void handleCommand(IoSession session, Command cmd) throws Exception {
            Object[] args = cmd.getArgs();

            switch (cmd.getType()) {
                case RECOVER:
                    log.debug("Got recover request from: " + session.getRemoteAddress());
                    Collection<UUID> taskIds = (Collection<UUID>)args[0];
                    for(UUID taskId : taskIds) {
                        log.debug("Reacquire task: " + taskId);
                        registerTask(session, taskId);
                        queue.acquireTask(taskId);
                    }

                    break;
                case READY:
                    sessionReady.add(session.getId());
                    synchronized (taskEmitter) {
                        taskEmitter.notifyAll();
                    }
                    break;
                case ACK:

                    if (!clientTask.get(session.getRemoteAddress()).contains(args[0])) {
                        throw new Exception("Can not ack task that was not first acquired: " + args[0] + " for " + session.getRemoteAddress());
                    } else {
                        log.debug("Got ACK for task " + args[0] + " from " + session.getRemoteAddress());
                        try {
                            queue.acknowledge((UUID) args[0]);
                        } finally {
                            endTask(session, (UUID)args[0]);
                        }

                    }
                    break;
                case NACK:

                    if (!clientTask.get(session.getRemoteAddress()).contains(args[0])) {
                        throw new Exception("Can not cancel task that was not first acquired: " + args[0] + " for " + session.getRemoteAddress());
                    } else {
                        log.debug("Got NACK for task " + args[0] + " from " + session.getRemoteAddress());
                        try {
                            queue.cancel((UUID) args[0], (Boolean) args[1]);
                        } finally {
                            endTask(session, (UUID)args[0]);
                        }
                    }

                    break;
                case ERROR:
                    if (!clientTask.get(session.getRemoteAddress()).contains(args[0])) {
                        throw new Exception("Error received for unacquired task: " + args[0] + " for " + session.getRemoteAddress());
                    } else {
                        log.debug("Got ERROR for task " + args[0] + " from " + session.getRemoteAddress());
                        try {
                            queue.failed((UUID) args[0]);
                        } finally {
                            endTask(session, (UUID)args[0]);
                        }
                    }
                    break;

            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            synchronized (clientTask) {
                clientTask.put(session.getRemoteAddress(), Collections.synchronizedList(new ArrayList<UUID>()));
                queue.setSubscribers(subscribers.incrementAndGet());

                log.debug(String.format("Got new client on " + session.getRemoteAddress() + ". Subscribers: " + queue.getSubscribers()));
            }

            synchronized (taskEmitter) {
                taskEmitter.notifyAll();
            }
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {

            synchronized (clientTask) {
                sessionReady.remove(session.getId());

                for(UUID taskId : clientTask.get(session.getRemoteAddress())) {
                    queue.cancel(taskId, true);
                    log.debug("Rescheduled task: " + taskId);
                }

                clientTask.remove(session.getRemoteAddress());
                queue.setSubscribers(subscribers.decrementAndGet());

                log.debug(String.format("Client connection dropped " + session.getRemoteAddress() + ". Subscribers: " + queue.getSubscribers()));

            }
        }


    }

    private class StaleTaskMonitor extends TimerTask {

        @Override
        public void run() {
            Iterator<T> running = queue.getStore().getRunning();

            List<UUID> staleIds = new ArrayList<UUID>();

            while(running.hasNext()) {
                T next = running.next();
                if (!requestHandler.taskIsRunning(next.getId())) {
                    staleIds.add(next.getId());
                }
            }

            for(UUID id : staleIds) {
                log.debug("Rescheduling stale task id: " + id);
                try {
                    queue.cancel(id,true);
                    requestHandler.unregisterTask(id);
                } catch (InterruptedException e) {}
            }

        }
    }

    private class TaskEmitter extends Thread {
        private TaskEmitter() {
            super("smartq-task-emitter");
        }

        private IoSession getNextSession() throws InterruptedException {
            LinkedList<IoSession> managedSessions = new LinkedList<IoSession>(acceptor.getManagedSessions().values());
            int sessionOffset = 0;

            while(true) {
                while (managedSessions.size() < 1) {
                    synchronized (this) {
                        wait();
                    }
                    managedSessions = new LinkedList<IoSession>(acceptor.getManagedSessions().values());
                }

                if (sessionOffset >= managedSessions.size()) {
                    sessionOffset = 0;
                    synchronized (this) {
                        log.debug("Waiting for new sessions to become available");
                        wait();
                    }
                    log.debug("Got new sessions - restarting");
                    managedSessions = new LinkedList<IoSession>(acceptor.getManagedSessions().values());
                    continue;
                }

                IoSession session = managedSessions.get(sessionOffset);
                sessionOffset++;

                if (session == null || requestHandler.isBusy(session)) {
                    continue;
                }

                if (requestHandler.isReady(session))
                    return session;
            }
        }

        @Override
        public void run() {
            try {
                while(!interrupted()) {

                    IoSession session = getNextSession();

                    try {
                        requestHandler.sendTask(session, queue.acquire());
                    } catch (AcquireInterruptedException ex) {
                        continue;
                    }
                }
            } catch (InterruptedException e) {}
        }
    }
}
