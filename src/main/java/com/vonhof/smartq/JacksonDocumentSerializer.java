package com.vonhof.smartq;


import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JacksonDocumentSerializer implements DocumentSerializer {
    private final ObjectMapper om = new ObjectMapper();

    @Override
    public String serialize(Object obj) throws IOException {
        return om.writeValueAsString(obj);
    }

    @Override
    public <T> T deserialize(String serialized,Class<T> clazz) throws IOException {
        return om.readValue(serialized, clazz);
    }
}
