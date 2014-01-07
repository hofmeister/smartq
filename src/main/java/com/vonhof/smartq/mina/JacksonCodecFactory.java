package com.vonhof.smartq.mina;

import com.vonhof.smartq.server.Command;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

public class JacksonCodecFactory extends StringBasedCodecFactory {
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

    protected String serialize(Object message) throws IOException {
        ObjectNode node = om.valueToTree(message);
        node.put(CLASS_FIELD,message.getClass().getName());
        return om.writeValueAsString(node);
    }

    protected Object deserialize(String json) throws IOException, ClassNotFoundException {
        ObjectNode node = (ObjectNode) om.readTree(json);
        String className = node.get(CLASS_FIELD).getTextValue();
        node.remove(CLASS_FIELD);
        return om.treeToValue(node, Class.forName(className));
    }

}
