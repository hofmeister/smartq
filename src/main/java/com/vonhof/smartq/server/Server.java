package com.vonhof.smartq.server;


import com.vonhof.smartq.SmartQ;
import com.vonhof.smartq.Task;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {

    private final InetSocketAddress address;
    private final SmartQ queue;

    private IoAcceptor acceptor;
    private static final AtomicInteger consumers = new AtomicInteger(0);


    public Server(InetSocketAddress address, SmartQ queue) {
        this.address = address;
        this.queue = queue;
    }


    public void listen() throws IOException {

        acceptor = new NioSocketAcceptor();

        ObjectSerializationCodecFactory codecFactory = new ObjectSerializationCodecFactory();
        codecFactory.setDecoderMaxObjectSize(Integer.MAX_VALUE);
        codecFactory.setEncoderMaxObjectSize(Integer.MAX_VALUE);

        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( codecFactory ));

        acceptor.setHandler( new RequestHandler() );

        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.bind( address );

    }

    public void close() {
        acceptor.dispose();
    }


    private class RequestHandler extends IoHandlerAdapter {

        @Override
        public void messageReceived(IoSession session, Object message) throws Exception {
            if (message instanceof Task) {
                handleTask((Task) message);
                session.write(true);
            }

            if (message instanceof Command) {
                handleCommand(session, (Command) message);
            }
        }

        private void handleTask(Task task) throws InterruptedException {
             queue.submit(task);
        }

        private void handleCommand(IoSession session, Command cmd) throws InterruptedException {
            Object[] args = cmd.getArgs();
            switch (cmd.getType()) {
                case RATE_LIMIT:
                    queue.setTaskTypeRateLimit((String)args[0], (Integer)args[1]);
                    session.write(true);
                    return;
                case ETA:
                    session.write(queue.getEstimatedTimeLeft());
                    break;
                case ETA_TYPE:
                    session.write(queue.getEstimatedTimeLeft((String)args[0]));
                    break;
            }
        }

        @Override
        public void sessionOpened(IoSession session) throws Exception {
            queue.setConsumers(consumers.incrementAndGet());
        }

        @Override
        public void sessionClosed(IoSession session) throws Exception {
            queue.setConsumers(consumers.decrementAndGet());
        }
    }
}
