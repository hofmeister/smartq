package com.vonhof.smartq.server;


import com.vonhof.smartq.AcquireInterruptedException;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.mina.JacksonCodecFactory;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
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

public class SmartQServer<T extends Task> {

    private static final Logger log = Logger.getLogger(SmartQServer.class);

    private final InetSocketAddress address;
    private final SmartQ<T,?> queue;

    private NioSocketAcceptor acceptor;
    private final AtomicInteger subscriberCount = new AtomicInteger(0);
    private final AtomicInteger clientCount = new AtomicInteger(0);

    private final RequestHandler requestHandler = new RequestHandler();
    private TaskEmitter taskEmitter;
    private final Timer timer = new Timer();
    private ProtocolCodecFactory protocolCodecFactory = new JacksonCodecFactory();



    public SmartQServer(InetSocketAddress address, SmartQ queue) {
        this.address = address;
        this.queue = queue;

    }

    public void setProtocolCodecFactory(ProtocolCodecFactory protocolCodecFactory) {
        this.protocolCodecFactory = protocolCodecFactory;
    }

    public int getSubscriberCount() {
        return queue.getSubscribers();
    }

    public int getClientCount() {
        return clientCount.get();
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public SmartQ<T,?> getQueue() {
        return queue;
    }

    public SmartQClient<T> makeClient() {
        return new SmartQClient<T>(address);
    }

    public SmartQClient<T> makeClient(SmartQClientMessageHandler<T> handler) {
        return new SmartQClient<T>(address, handler, 1);
    }

    public synchronized void listen() throws IOException {

        acceptor = new NioSocketAcceptor();
        acceptor.setReuseAddress(true);

        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( protocolCodecFactory ));

        acceptor.setHandler( requestHandler );

        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.setCloseOnDeactivation(true);
        acceptor.bind( address );

        if (log.isInfoEnabled()) {
            log.info(String.format("Listening on " + address));
        }

        taskEmitter = new TaskEmitter();
        taskEmitter.start();

        timer.scheduleAtFixedRate(new StaleTaskMonitor(),60000,300000);

    }

    public synchronized void close()  {
        acceptor.unbind();
        acceptor.dispose(true);
        acceptor = null;

        if (log.isInfoEnabled()) {
            log.info(String.format("Stopped listening on " + address));
        }
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
        private final Map<SocketAddress,Integer> clientTaskLimit = new ConcurrentHashMap<SocketAddress, Integer>();
        private final Set<Long> sessionReady = new ConcurrentHashSet<Long>();
        private final Set<UUID> taskIds = new ConcurrentHashSet<UUID>();

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
            log.error("Got exception while handling request", cause);
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            if (log.isTraceEnabled()) {
                log.trace("Received message: " + message);
            }
            if (message instanceof Command) {
                handleCommand(session, (Command) message);
                return;
            }

            session.write(new Error("Invalid message: " + message));
        }

        public boolean taskIsRunning(UUID id) {
            return taskIds.contains(id);
        }

        private int getTaskLimit(IoSession session) {
            if (clientTaskLimit.containsKey(session.getRemoteAddress())) {
                return clientTaskLimit.get(session.getRemoteAddress());
            }
            return 1;
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
            return tasks == null || (tasks.size() >= getTaskLimit(session));
        }

        public void sendTask(IoSession session, T task) throws InterruptedException {
            if (!isAlive(session)) {
                return;
            }
            while(isAlive(session) && isBusy(session)) {
                if (clientTask.get(session.getRemoteAddress()).contains(task.getId())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for client to tell us what it knows: " + session.getRemoteAddress());
                    }
                    break; //We are resending something
                }

                synchronized (clientTask) {
                    clientTask.wait(5000);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Waiting for client to become available: " + session.getRemoteAddress());
                }
            }
            if (!isAlive(session)) {
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Sending task to client: " + session.getRemoteAddress() + " - " + task.getId());
            }
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
                    if (log.isDebugEnabled()) {
                        log.debug("Got recover request from: " + session.getRemoteAddress());
                    }
                    Collection<UUID> taskIds = (Collection<UUID>)args[0];
                    for(UUID taskId : taskIds) {
                        if (log.isDebugEnabled()) {
                            log.debug("Reacquire task: " + taskId);
                        }
                        registerTask(session, taskId);
                        queue.acquireTask(taskId);
                    }

                    break;
                case SUBSCRIBE:
                    if (log.isTraceEnabled()) {
                        log.trace("Client subscribing: " + session.getId());
                    }

                    if (!sessionReady.contains(session.getId())) {
                        if (args.length > 0 && args[0] instanceof Integer) {
                            clientTaskLimit.put(session.getRemoteAddress(), (Integer) args[0]);
                        }

                        sessionReady.add(session.getId());
                        synchronized (taskEmitter) {
                            taskEmitter.notifyAll();
                        }

                        queue.setSubscribers(subscriberCount.incrementAndGet());

                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Client started subscribing to tasks: %s with %s threads . Subscribers: %s",session.getRemoteAddress(), getTaskLimit(session), queue.getSubscribers()));
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("Session already found: " + session.getId());
                        }
                    }
                    break;
                case ACK:

                    if (!clientTask.get(session.getRemoteAddress()).contains(args[0])) {
                        throw new Exception("Can not ack task that was not first acquired: " + args[0] + " for " + session.getRemoteAddress());
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Got ACK for task " + args[0] + " from " + session.getRemoteAddress());
                        }
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
                        if (log.isDebugEnabled()) {
                            log.debug("Got NACK for task " + args[0] + " from " + session.getRemoteAddress());
                        }
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
                        if (log.isDebugEnabled()) {
                            log.debug("Got ERROR for task " + args[0] + " from " + session.getRemoteAddress());
                        }
                        try {
                            queue.failed((UUID) args[0]);
                        } finally {
                            endTask(session, (UUID)args[0]);
                        }
                    }
                    break;
                case PUBLISH:
                    queue.submit((T) args[0]);
                    break;

            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            synchronized (clientTask) {
                clientCount.incrementAndGet();

                clientTask.put(session.getRemoteAddress(), Collections.synchronizedList(new ArrayList<UUID>()));
                if (log.isDebugEnabled()) {
                    log.debug("Got new client on " + session.getRemoteAddress() + ".");
                }
            }

            synchronized (taskEmitter) {
                taskEmitter.notifyAll();
            }
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {

            synchronized (clientTask) {
                clientCount.decrementAndGet();

                if (sessionReady.contains(session.getId())) {
                    queue.setSubscribers(subscriberCount.decrementAndGet());
                }

                sessionReady.remove(session.getId());

                for(UUID taskId : clientTask.get(session.getRemoteAddress())) {
                    queue.cancel(taskId, true);
                    if (log.isDebugEnabled()) {
                        log.debug("Rescheduled task: " + taskId);
                    }
                }

                clientTask.remove(session.getRemoteAddress());
                clientTaskLimit.remove(session.getRemoteAddress());

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Client connection dropped %s. Subscribers: ",session.getRemoteAddress(), queue.getSubscribers()));
                }

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
                if (log.isDebugEnabled()) {
                    log.debug("Rescheduling stale task id: " + id);
                }
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
                        if (log.isDebugEnabled()) {
                            log.debug("Waiting for new sessions to become available");
                        }
                        wait();
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Got new sessions - refreshing session list");
                    }
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
