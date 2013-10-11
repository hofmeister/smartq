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
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SmartQConsumer<T extends Task> {

    private static final Logger log = Logger.getLogger(SmartQConsumer.class);

    private IoSession session = null;
    private NioSocketConnector connector = null;

    private volatile boolean closing = false;
    private volatile boolean reconnecting = false;

    private final InetSocketAddress hostAddress;

    private int connectionTimeout = 30000;
    private int retryTimeout = 5000;

    private final SmartQConsumerHandler<T> responseHandler;

    private Map<UUID, T> activeTasks = new ConcurrentHashMap<UUID, T>();
    private List<Command> queuedMessages = Collections.synchronizedList(new LinkedList<Command>());

    public SmartQConsumer(InetSocketAddress hostAddress, SmartQConsumerHandler<T> responseHandler) {
        this.hostAddress = hostAddress;
        this.responseHandler = responseHandler;
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

        connector = new NioSocketConnector();
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));

        connector.getFilterChain().addLast("logger", new LoggingFilter());

        connector.setHandler(new ClientSessionHandler());

        try {
            ConnectFuture future = connector.connect(hostAddress);
            future.await(connectionTimeout);
            session = future.getSession();

            log.debug(String.format("Connected to " + hostAddress));
        } catch (RuntimeIoException e) {
            connector.dispose();
            connector = null;
            session = null;
            throw new IOException("Could not connect to "+hostAddress,e);

        }
    }

    public void close() {
        closing = true;

        if (session != null) {
            session.close(true);
        }

        session = null;
        if (connector != null) {
            connector.dispose(false);
            log.debug(String.format("Disconnected from " + hostAddress));
        }
        connector = null;
    }

    protected void reconnectLater() throws InterruptedException {
        if (closing) {
            closing = false;
            return;
        }
        new Thread("Recover thread") {
            @Override
            public void run() {
                reconnecting = true;
                close();


                if (retryTimeout < 1) {
                    return;
                }

                log.debug("Was unexpectedly disconnected from  " + hostAddress+". Will try to reconnect.");

                while(true) {
                    try {
                        connect();
                        reconnecting = false;
                        recover();
                    } catch (IOException e) {
                        log.debug("Failed to reconnect to " + hostAddress+". Waiting "+retryTimeout+"ms before trying again");
                        try {
                            Thread.sleep(retryTimeout);
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

    private synchronized void recover() throws InterruptedException {
        if (!activeTasks.isEmpty()) {
            if (!send(new Command(Type.RECOVER, new ArrayList<UUID>(activeTasks.keySet())))) {
                throw new RuntimeException("Timed out while waiting for recover");
            }
        }
        if (!queuedMessages.isEmpty()) {
            for(Command cmd: queuedMessages) {
                send(cmd);
            }

            queuedMessages.clear();
        }
    }

    public void acquire() throws InterruptedException {
        send(new Command(Type.ACQUIRE));
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
        } else if (reconnecting && !message.getType().equals(Type.RECOVER)) {
            queuedMessages.add(message);
        } else {
            throw new RuntimeIoException("Connection not available");
        }

        return false;
    }

    private class ClientSessionHandler implements IoHandler {

        @Override
        public void sessionCreated(IoSession session) throws Exception {}
        @Override
        public void sessionOpened(IoSession session) throws Exception {}
        @Override
        public void sessionIdle(IoSession session, IdleStatus status) throws Exception {}
        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
            log.error("Got exception while processing request", cause);
            if (cause instanceof IOException) {
                reconnectLater();
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
                T task = (T) message;
                try {
                    activeTasks.put(task.getId(),task);
                    responseHandler.taskReceived(SmartQConsumer.this, task);
                } catch (Exception ex) {
                    log.error("Failed to process task",ex);
                    cancel(((T)message).getId(),false);
                }
            }

            if (message instanceof Error) {
                throw new Exception(((Error)message).getMessage());
            }
        }
    }
}
