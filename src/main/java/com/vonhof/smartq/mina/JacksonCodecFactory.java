package com.vonhof.smartq.mina;

import com.vonhof.smartq.server.Command;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.node.ObjectNode;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class JacksonCodecFactory implements ProtocolCodecFactory {

    private ObjectMapper om = new ObjectMapper();

    private final static String CLASS_FIELD = "bs__className";

    public JacksonCodecFactory() {
        addDeserializers();
    }

    private void addDeserializers() {
        SimpleModule module =
                new SimpleModule("SmartqDeserizliser",
                        new Version(1, 0, 0, null));

        module.addDeserializer(Command.class, new CommandDeserializer());
        om.registerModule(module);
    }

    @Override
    public ProtocolEncoder getEncoder(IoSession session) throws Exception {
        return new JacksonEncoder(om);
    }

    @Override
    public ProtocolDecoder getDecoder(IoSession session) throws Exception {
        return new JacksonDecoder(om);
    }


    public static class JacksonEncoder implements ProtocolEncoder {

        private final ObjectMapper om;
        private final CharsetEncoder encoder;

        public JacksonEncoder(ObjectMapper om) {
            this.om = om;
            encoder = Charset.forName("UTF-8").newEncoder();
        }


        @Override
        public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {

            ObjectNode node = om.valueToTree(message);
            node.put(CLASS_FIELD,message.getClass().getName());

            String json = om.writeValueAsString(node);

            IoBuffer buf = IoBuffer.allocate(json.length()).setAutoExpand(true);

            buf.putInt(json.length());
            buf.putString(json, encoder);
            buf.flip();

            out.write(buf);
            out.flush();
        }

        @Override
        public void dispose(IoSession session) throws Exception {

        }
    }

    public static class JacksonDecoder implements ProtocolDecoder {

        private final ObjectMapper om;
        private final CharsetDecoder decoder;

        public JacksonDecoder(ObjectMapper om) {
            this.om = om;
            decoder = Charset.forName("UTF-8").newDecoder();
        }

        @Override
        public void decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
            int stringLength = in.getInt();
            String json = in.getString(stringLength, decoder);

            ObjectNode node = (ObjectNode) om.readTree(json);
            String className = node.get(CLASS_FIELD).getTextValue();
            node.remove(CLASS_FIELD);

            out.write(om.treeToValue(node, Class.forName(className)));
        }

        @Override
        public void finishDecode(IoSession session, ProtocolDecoderOutput out) throws Exception {

        }

        @Override
        public void dispose(IoSession session) throws Exception {

        }
    }
}
