package com.vonhof.smartq.mina;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

abstract public class StringBasedCodecFactory implements ProtocolCodecFactory {

    @Override
    public ProtocolEncoder getEncoder(IoSession session) throws Exception {
        return new StringEncoder();
    }

    @Override
    public ProtocolDecoder getDecoder(IoSession session) throws Exception {
        return new StringDecoder();
    }

    abstract protected String serialize(Object message) throws Exception;

    abstract protected Object deserialize(String json) throws Exception;


    public class StringEncoder  extends ProtocolEncoderAdapter {


        private CharsetEncoder encoder;

        public StringEncoder() {
            encoder = Charset.forName("UTF-8").newEncoder();
        }

        @Override
        public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {

            String serializedMessage = serialize(message);

            IoBuffer buf = IoBuffer.allocate(serializedMessage.length()).setAutoExpand(true);

            buf.putPrefixedString(serializedMessage, 4, encoder);
            buf.flip();

            out.write(buf);
        }
    }

    public class StringDecoder extends CumulativeProtocolDecoder {

        CharsetDecoder decoder;

        public StringDecoder() {
            decoder = Charset.forName("UTF-8").newDecoder();
        }

        @Override
        protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
            if (!in.prefixedDataAvailable(4, Integer.MAX_VALUE)) {
                return false;
            }

            String serializedMessage = in.getPrefixedString(4, decoder);

            Object message = deserialize(serializedMessage);

            out.write(message);

            return true;
        }
    }
}
