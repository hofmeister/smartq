package com.vonhof.smartq.server;


import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SmartQProducer<T extends Task> {

    private static final Logger log = Logger.getLogger(SmartQProducer.class);

    private final InetSocketAddress address;
    private final SmartQ<T,?> queue;

    private NioSocketAcceptor acceptor;
    private static final AtomicInteger consumers = new AtomicInteger(0);


    public SmartQProducer(InetSocketAddress address, SmartQ queue) {
        this.address = address;
        this.queue = queue;
    }

    public int getConsumerCount() {
        return consumers.get();
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public SmartQ<T,?> getQueue() {
        return queue;
    }

    public SmartQConsumer<T> makeConsumer(SmartQConsumerHandler<T> handler) {
        return new SmartQConsumer<T>(address, handler);
    }

    public synchronized void listen() throws IOException {

        acceptor = new NioSocketAcceptor();
        acceptor.setReuseAddress(true);

        ObjectSerializationCodecFactory codecFactory = new ObjectSerializationCodecFactory();
        codecFactory.setDecoderMaxObjectSize(Integer.MAX_VALUE);
        codecFactory.setEncoderMaxObjectSize(Integer.MAX_VALUE);

        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( codecFactory ));
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter());

        acceptor.setHandler( new RequestHandler() );


        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.setCloseOnDeactivation(true);
        acceptor.bind( address );

        log.debug(String.format("Listening on " + address));
    }

    public synchronized void close() {
        acceptor.unbind();
        acceptor.dispose(true);
        acceptor = null;

        log.debug(String.format("Stopped listening on " + address));
    }


    private class RequestHandler extends IoHandlerAdapter {
        private final Map<UUID,T> tasks = new ConcurrentHashMap<UUID, T>();

        @Override
        public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
            log.error("Got exception while handling request", cause);
        }

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            if (message instanceof Task) {
                handleTask((T) message);
                return;
            }

            if (message instanceof Command) {
                handleCommand(session, (Command) message);
                return;
            }

            session.write(new Error("Invalid message: " + message));
        }


        private void handleTask(T task) throws InterruptedException {
             queue.submit(task);
        }

        private void handleCommand(IoSession session, Command cmd) throws Exception {
            Object[] args = cmd.getArgs();
            T task = null;
            switch (cmd.getType()) {
                case RECOVER:
                    Collection<UUID> taskIds = (Collection<UUID>)args[0];
                    for(UUID taskId : taskIds) {
                        T t = queue.acquireTask(taskId);
                        if (t != null) {
                            log.debug("Client reacquired task: " + t.getId());
                            tasks.put(taskId, t);
                            session.write(t);
                        }
                    }

                    break;
                case ACQUIRE:
                    task = queue.acquire();
                    tasks.put(task.getId(), task);
                    log.debug("Client acquired task: " + task.getId());
                    session.write(task);
                    break;
                case ACK:
                    log.debug("Got ACK from client: " + args[0]);
                    if (!tasks.containsKey(args[0])) {
                        throw new Exception("Can not ack task that was not first acquired");
                    } else {
                        tasks.remove(args[0]);
                        queue.acknowledge((UUID) args[0]);
                    }
                    break;
                case NACK:
                    log.debug("Got NACK from client: " + args[0]);
                    if (!tasks.containsKey(args[0])) {
                        throw new Exception("Can not cancel task that was not first acquired");
                    } else {
                        tasks.remove(args[0]);
                        queue.cancel((UUID) args[0], (Boolean) args[1]);
                    }

                    break;

            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            log.debug(String.format("Got new client on " + session.getRemoteAddress()));
            queue.setConsumers(consumers.incrementAndGet());
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            log.debug(String.format("Client connection dropped " + session.getRemoteAddress()));
            queue.setConsumers(consumers.decrementAndGet());
            for(T task : tasks.values()) {
                queue.cancel(task, true);
            }
        }
    }
}
