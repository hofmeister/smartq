package com.vonhof.smartq.mina;

import com.vonhof.smartq.server.Command;
import com.vonhof.smartq.server.Command.Type;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CommandDeserializer extends JsonDeserializer<Command> {

    private ObjectMapper om = new ObjectMapper();

    @Override
    public Command deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode json = jp.readValueAsTree();


        final Type type = Type.valueOf(json.get("type").getTextValue());
        final List<Object> argList = new LinkedList<Object>();

        ArrayNode args = (ArrayNode) json.get("args");
        if (args.isArray() && args.size() > 0) {
            int i = 0;
            for(JsonNode arg : args) {
                argList.add(om.treeToValue(arg, type.getArgTypes()[i]));
                i++;
            }
        }

        Command cmd = new Command(type, argList.toArray());;

        System.out.println(cmd.toString());

        return cmd;
    }
}
