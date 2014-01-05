package com.vonhof.smartq.server;


import com.vonhof.smartq.Task;
import com.vonhof.smartq.server.Command.Type;
import org.apache.log4j.Logger;
import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SmartQClient<T extends Task> {

    private static final Logger log = Logger.getLogger(SmartQClient.class);


    private IoSession session = null;
    private NioSocketConnector connector = null;

    private volatile boolean closing = false;
    private volatile boolean reconnecting = false;

    private final InetSocketAddress hostAddress;

    private int connectionTimeout = 30000;
    private int retryTimeout = 5000;

    private final SmartQClientMessageHandler<T> responseHandler;

    private final Map<UUID, T> activeTasks = new ConcurrentHashMap<UUID, T>();
    private final List<Command> queuedMessages = Collections.synchronizedList(new LinkedList<Command>());
    private final Executor executor;
    private final int threads;
    private Timer timer;

    private final UUID id;


    /**
     * Creates a publish / subscribe queue client.
     * @param hostAddress host to connect to
     * @param responseHandler the handler will receive all tasks
     * @param threads Determines how many concurrent tasks can be handled. Defaults to available processors
     */
    public SmartQClient(InetSocketAddress hostAddress, SmartQClientMessageHandler<T> responseHandler, int threads) {
        id = UUID.randomUUID();
        this.hostAddress = hostAddress;
        this.responseHandler = responseHandler;
        if (threads > 0) {
            this.executor = Executors.newFixedThreadPool( threads );
        } else {
            this.executor = null;
        }
        this.threads = threads;
    }

    /**
     * Creates a publish / subscribe queue client.
     * @param hostAddress host to connect to
     * @param responseHandler the handler will receive all tasks
     */
    public SmartQClient(InetSocketAddress hostAddress, SmartQClientMessageHandler<T> responseHandler) {
        this(hostAddress, responseHandler, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates a publish-only client (will not receive messages)
     * @param hostAddress host to connect to
     */
    public SmartQClient(InetSocketAddress hostAddress) {
        this(hostAddress, null, 0);
    }

    public int getRetryTimeout() {
        return retryTimeout;
    }

    public void setRetryTimeout(int retryTimeout) {
        this.retryTimeout = retryTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void connect() throws IOException, InterruptedException {
        if (connector != null) {
            throw new RuntimeException("Client already connected. Close connection before reconnecting");
        }

        timer = new Timer();
        connector = new NioSocketConnector();
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

        connector.setHandler(new ClientSessionHandler());

        try {
            ConnectFuture future = connector.connect(hostAddress);
            future.await(connectionTimeout);
            session = future.getSession();

            if (log.isDebugEnabled()) {
                log.debug("Connected to " + hostAddress + " - " + this);
            }

            if (!reconnecting) {
                subscribe();
            }
            timer.scheduleAtFixedRate(new HostPinger(),5000,1000);
        } catch (RuntimeIoException e) {
            if (connector != null) {
                connector.dispose();
            }

            connector = null;
            session = null;
            throw new IOException("Could not connect to "+hostAddress,e);

        }
    }

    public void close() {
        if (timer != null) {
            timer.cancel();
            timer.purge();
            timer = null;
        }


        closing = true;
        if (session != null) {
            session.close(true);
        }

        session = null;
        if (connector != null) {
            connector.dispose(false);
            if (!reconnecting) {
                if (log.isDebugEnabled()) {
                    log.debug("Disconnected from " + hostAddress + " - " + this);
                }
            }
        }
        connector = null;
    }

    protected synchronized void reconnectLater() {
        if (closing) {
            closing = false;
            return;
        }
        if (reconnecting) {
            return;
        }
        reconnecting = true;
        new Thread("Recover thread") {
            @Override
            public void run() {

                close();
                closing = false;


                if (retryTimeout < 1) {
                    return;
                }

                log.warn("Was unexpectedly disconnected from  " + hostAddress+". Will try to reconnect. " + SmartQClient.this);

                while(true) {
                    try {
                        connect();
                        reconnecting = false;
                        recover();
                        subscribe();
                    } catch (IOException e) {
                        if (log.isTraceEnabled()) {
                            log.trace("Failed to reconnect to " + hostAddress + ". Waiting " + retryTimeout + "ms before trying again");
                        }
                        try {
                            synchronized (this) {
                                wait(retryTimeout);
                            }
                        } catch (InterruptedException e1) {
                            return;
                        }

                        continue;
                    } catch (Exception e) {
                        log.warn("Could not reconnect", e);
                        return;
                    }
                    break;
                }
            }
        }.start();
    }

    public void publish(T task) throws InterruptedException {
        send(new Command(Type.PUBLISH, task));
    }

    /**
     * Send SUBSCRIBE command to server. Only sends this if a response handler is present (SUBSCRIBE indicates we are ready for
     * messages).
     * @throws InterruptedException
     */
    private synchronized void subscribe() throws InterruptedException {
        if (responseHandler != null) {
            send(new Command(Type.SUBSCRIBE, threads));
        }
    }

    private synchronized void recover() throws InterruptedException {
        if (!checkSession()) {
            return;
        }
        if (!activeTasks.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Sending recover request to newly opened host: " + activeTasks.size());
            }

            if (!send(new Command(Type.RECOVER, new ArrayList<UUID>(activeTasks.keySet())))) {
                throw new RuntimeException("Timed out while waiting for recover");
            }
            activeTasks.clear();
        }
        if (!queuedMessages.isEmpty()) {
            List<Command> cmds = new LinkedList<Command>(queuedMessages);

            if (log.isDebugEnabled()) {
                log.debug("Executing queued messages: " + queuedMessages.size());
            }

            queuedMessages.clear();
            for(Command cmd: cmds) {
                switch (cmd.getType()) {
                    case ACK:
                        acknowledge((UUID)cmd.getArgs()[0]);
                        break;
                    case NACK:
                        cancel((UUID)cmd.getArgs()[0],(Boolean)cmd.getArgs()[0]);
                        break;
                    case SUBSCRIBE:
                        subscribe();
                        break;
                }
            }
        }
    }

    public void acknowledge(UUID taskId) throws InterruptedException {
        if (send(new Command(Type.ACK, taskId))) {
            activeTasks.remove(taskId);
        }
    }

    public void cancel(UUID taskId, boolean requeue) throws InterruptedException {
        if (send(new Command(Type.NACK, taskId, requeue))) {
            activeTasks.remove(taskId);
        }
    }

    public void failed(UUID taskId) throws InterruptedException {
        if (send(new Command(Type.ERROR, taskId))) {
            activeTasks.remove(taskId);
        }
    }

    private boolean checkSession() {
        if (session == null || connector == null) {
            return false;
        }

        if (!session.isConnected()
                || session.isClosing()
                || connector.isDisposed()
                || connector.isDisposing()) {
            return false;
        }

        return true;
    }

    private boolean send(Command message) throws InterruptedException {
        if (checkSession()) {
            session.write(message);
            return true;
        } else if (!message.getType().equals(Type.RECOVER)) {
            if (log.isInfoEnabled()) {
                log.info("Connection unavailable - command queued: " + message);
            }
            queuedMessages.add(message);
            if (!reconnecting) {
                reconnectLater();
            }
        } else {
            throw new RuntimeIoException("Connection not available - " + this);
        }

        return false;
    }

    @Override
    public String toString() {
        return "C{" + id + '}';
    }

    private class ClientSessionHandler implements IoHandler {

        @Override
        public void sessionCreated(IoSession session) throws Exception {}

        @Override
        public void sessionOpened(IoSession session) throws Exception {

        }

        @Override
        public void sessionIdle(IoSession session, IdleStatus status) throws Exception {}
        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
            if (cause instanceof IOException) {
                reconnectLater();
            } else {
                log.error("Got exception while processing request on " + SmartQClient.this, cause);
            }
        }
        @Override
        public void messageSent(IoSession session, Object message) throws Exception {}

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            reconnectLater();
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            if (message instanceof Task) {
                final T task = (T) message;

                if (log.isDebugEnabled()) {
                    log.debug("Processing task: " + task.getId() + " on " + SmartQClient.this);
                }

                activeTasks.put(task.getId(),task);

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseHandler.taskReceived(SmartQClient.this, task);
                            if (log.isDebugEnabled()) {
                                log.debug("Task processed: " + task.getId() + " on " + SmartQClient.this);
                            }
                        } catch (Exception ex) {
                            log.error("Failed to process task:" + task.getId() + " on " + SmartQClient.this, ex);

                            try {
                                failed(task.getId());
                            } catch (InterruptedException e) {}
                        }
                    }
                });

            }

            if (message instanceof Error) {
                throw new Exception(((Error)message).getMessage());
            }
        }
    }

    private class HostPinger extends TimerTask {

        @Override
        public void run() {
            if (!checkSession()) {
                reconnectLater();
            }
        }
    }
}
