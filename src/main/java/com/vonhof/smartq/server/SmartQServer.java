package com.vonhof.smartq.server;


import com.vonhof.smartq.AcquireInterruptedException;
import com.vonhof.smartq.CountMap;
import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import com.vonhof.smartq.mina.JacksonCodecFactory;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
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

public class SmartQServer {

    private static final Logger log = Logger.getLogger(SmartQServer.class);

    private final InetSocketAddress address;
    private final SmartQ<?> queue;

    private NioSocketAcceptor acceptor;
    private final AtomicInteger subscriberCount = new AtomicInteger(0);
    private final AtomicInteger clientCount = new AtomicInteger(0);
    private final CountMap<String> clientCountForGroup = new CountMap<>();
    private final CountMap<String> subscriberCountForGroup = new CountMap<>();

    private final RequestHandler requestHandler = new RequestHandler();
    private TaskEmitter taskEmitter;
    private final Timer timer = new Timer("smartq-timer");
    private ProtocolCodecFactory protocolCodecFactory = new JacksonCodecFactory();



    public SmartQServer(InetSocketAddress address, SmartQ queue) {
        this.address = address;
        this.queue = queue;

    }

    protected SmartQServer() {
        address = null;
        queue = null;
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

    public long getClientCountForGroup(String group) {
        return clientCountForGroup.get(group);
    }

    public long getSubscriberCountForGroup(String group) {
        return subscriberCountForGroup.get(group);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public SmartQ<?> getQueue() {
        return queue;
    }

    public SmartQClient makeClient() {
        return new SmartQClient(address);
    }

    public SmartQClient makeClient(SmartQClientMessageHandler handler) {
        return new SmartQClient(address, handler, 1);
    }

    public synchronized void listen() throws IOException {

        acceptor = new NioSocketAcceptor();
        acceptor.setReuseAddress(true);

        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( protocolCodecFactory ));

        acceptor.setHandler( requestHandler );

        acceptor.getSessionConfig().setReadBufferSize( 1024 );

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
        if (taskEmitter != null) {
            taskEmitter.interrupt();

            try {
                taskEmitter.join();
            } catch (InterruptedException e) {

            }
            taskEmitter = null;
        }

        acceptor.unbind();
        acceptor.dispose(true);
        acceptor = null;

        if (log.isInfoEnabled()) {
            log.info(String.format("Stopped listening on " + address));
        }
        queue.interrupt();

        timer.cancel();
        timer.purge();

    }




    private class RequestHandler extends IoHandlerAdapter {
        private final Map<SocketAddress,List<UUID>> clientTask = new ConcurrentHashMap<>();
        private final Map<SocketAddress,Integer> clientTaskLimit = new ConcurrentHashMap<>();

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

        private void clientTasksChanged() {
            synchronized (clientTask) {
                clientTask.notifyAll();
            }
        }

        public void registerTask(IoSession session, UUID id) {
            clientTask.get(session.getRemoteAddress()).add(id);
            taskIds.add(id);

            clientTasksChanged();
        }

        public int getTaskCountForSession(IoSession session) {
            return clientTask.get(session.getRemoteAddress()).size();
        }

        public void unregisterTask(IoSession session, UUID id) {
            clientTask.get(session.getRemoteAddress()).remove(id);
            taskIds.remove(id);

            clientTasksChanged();
        }

        public void unregisterTask(UUID id) {
            for(Entry<SocketAddress, List<UUID>> entry : clientTask.entrySet()) {
                if (entry.getValue().contains(id)) {
                    entry.getValue().remove(id);
                    break;
                }
            }

            taskIds.remove(id);

            clientTasksChanged();
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

        public boolean sendTask(IoSession session, Task task) throws InterruptedException {
            if (!isAlive(session)) {
                return false;
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
                return false;
            }

            if (log.isDebugEnabled()) {
                log.debug("Sending task to client: " + session.getRemoteAddress() + " - " + task.getId());
            }

            registerTask(session, task.getId());
            session.write(task);
            return true;
        }

        public void endTask(IoSession session, UUID taskId) {
            if (!isAlive(session)) {
                return;
            }
            unregisterTask(session, taskId);
            taskEmitter.checkForSessions();
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
                        queue.markAsRunning(taskId);
                    }

                    break;
                case SUBSCRIBE:
                    if (log.isTraceEnabled()) {
                        log.trace("Client subscribing. (SessionId: " + session.getId() + ")");
                    }

                    if (!sessionReady.contains(session.getId())) {
                        clientTaskLimit.put(session.getRemoteAddress(), 1);
                        if (args.length > 0 && args[0] instanceof Integer) {
                            clientTaskLimit.put(session.getRemoteAddress(), (Integer) args[0]);
                        }

                        session.setAttribute("GROUP", SmartQ.GROUP_DEFAULT);
                        if (args.length > 1 && args[1] instanceof String) {
                            session.setAttribute("GROUP", args[1]);
                        }

                        clientCountForGroup.increment(session.getAttribute("GROUP").toString(), 1);

                        sessionReady.add(session.getId());
                        taskEmitter.checkForSessions();

                        int taskLimit = clientTaskLimit.get(session.getRemoteAddress());
                        queue.setSubscribers(subscriberCount.addAndGet(taskLimit));

                        subscriberCountForGroup.increment(session.getAttribute("GROUP").toString(), taskLimit);

                        if (log.isInfoEnabled()) {
                            log.info(String.format("Client started subscribing to tasks: %s with %s threads . Subscribers: %s [Group: %s]",
                                    session.getRemoteAddress(),
                                    getTaskLimit(session),
                                    queue.getSubscribers(),
                                    session.getAttribute("GROUP")));
                        }
                    } else {
                        if (log.isInfoEnabled()) {
                            log.info("Session already found: " + session.getId());
                        }
                    }

                    clientTasksChanged();
                    taskEmitter.checkForSessions();

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
                    queue.submit((Task) args[0]);
                    break;
                case CANCEL_REF:
                    queue.cancelByReference((String) args[0]);
                    break;

            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            synchronized (clientTask) {
                clientCount.incrementAndGet();

                clientTask.put(session.getRemoteAddress(), Collections.synchronizedList(new ArrayList<UUID>()));
                if (log.isInfoEnabled()) {
                    log.info("Got new client on " + session.getRemoteAddress() + ". Subscribers: " + clientCount.get());
                }
            }

            taskEmitter.checkForSessions();
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {

            synchronized (clientTask) {
                clientCount.decrementAndGet();
                Integer taskLimit = clientTaskLimit.get(session.getRemoteAddress());

                if (sessionReady.contains(session.getId())) {
                    queue.setSubscribers(subscriberCount.addAndGet(taskLimit == null ? -1 :  (taskLimit*-1)));

                    clientCountForGroup.decrement(session.getAttribute("GROUP").toString(), 1);
                    subscriberCountForGroup.decrement(session.getAttribute("GROUP").toString(), taskLimit);
                }

                sessionReady.remove(session.getId());

                for(UUID taskId : clientTask.get(session.getRemoteAddress())) {
                    queue.cancel(taskId, true);
                    if (log.isInfoEnabled()) {
                        log.info("Rescheduled task: " + taskId);
                    }
                }

                clientTask.remove(session.getRemoteAddress());
                clientTaskLimit.remove(session.getRemoteAddress());

                if (log.isInfoEnabled()) {
                    log.info(String.format("Client connection dropped %s. Subscribers: %s",session.getRemoteAddress(), queue.getSubscribers()));
                }

            }
        }


    }

    private class StaleTaskMonitor extends TimerTask {

        @Override
        public void run() {
            Iterator<Task> running = queue.getStore().getRunning();

            List<UUID> staleIds = new ArrayList<UUID>();

            while(running.hasNext()) {
                Task next = running.next();
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

        private boolean anySessionsAvailable()  {
            LinkedList<IoSession> managedSessions = new LinkedList<>(acceptor.getManagedSessions().values());

            if (managedSessions.isEmpty()) {
                return false;
            }

            for (IoSession managedSession : managedSessions) {
                if (requestHandler.isBusy(managedSession) ||
                        !requestHandler.isReady(managedSession)) {
                    continue;
                }

                return true;
            }

            return false;
        }

        private IoSession getNextSession(String group) throws InterruptedException {
            LinkedList<IoSession> managedSessions = new LinkedList<>(acceptor.getManagedSessions().values());

            if (managedSessions.isEmpty()) {
                return null;
            }

            int leastBusySessionCount = -1;
            IoSession session = null;

            for (IoSession managedSession : managedSessions) {

                if (!group.equals(managedSession.getAttribute("GROUP"))) {
                    continue;
                }

                if (requestHandler.isBusy(managedSession) ||
                        !requestHandler.isReady(managedSession)) {
                    continue;
                }

                int taskCount = requestHandler.getTaskCountForSession(managedSession);
                if (taskCount < 1) {
                    session = managedSession;
                    break;
                }

                if (leastBusySessionCount == -1 ||
                        leastBusySessionCount > taskCount) {
                    session = managedSession;
                    leastBusySessionCount = taskCount;
                }
            }

            if (session != null &&
                    requestHandler.isReady(session)) {
                return session;
            }

            if (!group.equals(SmartQ.GROUP_DEFAULT)) {
                return getNextSession(SmartQ.GROUP_DEFAULT);
            }

            return null;
        }

        public void checkForSessions() {
            synchronized (this) {
                notifyAll();
            }
        }

        private void waitForAnySession() throws InterruptedException {
            while(true) {
                synchronized (this) {
                    if (log.isInfoEnabled()) {
                        log.info("Waiting for sessions to become available");
                    }
                    wait(15000);
                }

                if (anySessionsAvailable()) {
                    break;
                }
            }
        }



        @Override
        public void run() {
            try {
                while(!interrupted()) {

                    if (!anySessionsAvailable()) {
                        waitForAnySession();
                        continue;
                    }

                    try {
                        Task task = queue.acquire();
                        IoSession session = getNextSession(task.getGroup());
                        if (session == null) {
                            queue.cancel(task, true);
                            continue;
                        }
                        requestHandler.sendTask(session, task);
                    } catch (AcquireInterruptedException ex) {
                        continue;
                    }
                }
            } catch (InterruptedException e) {}
        }


    }
}
