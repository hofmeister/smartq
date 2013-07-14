package com.vonhof.smartq;


import java.io.IOException;

public interface DocumentSerializer {

    public String serialize(Object obj) throws IOException;
    public <T> T deserialize(String serialized, Class<T> clazz) throws IOException;
}
