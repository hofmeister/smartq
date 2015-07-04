package com.vonhof.smartq.server;


import com.vonhof.smartq.Task;

import java.util.Arrays;
import java.util.UUID;

public class Command {
    private final Type type;
    private final Object[] args;

    public Command(Type type, Object ... args) {
        this.type = type;
        this.args = args;
    }

    public Type getType() {
        return type;
    }

    public Object[] getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return "Command{" +
                "type=" + type +
                ", args=" + Arrays.toString(args) +
                '}';
    }

    public static enum Type {
        SUBSCRIBE(Integer.class, String.class),
        ACK(UUID.class),
        NACK(UUID.class, Boolean.class),
        RECOVER(UUIDList.class),
        ERROR(UUID.class),
        PUBLISH(Task.class),
        CANCEL_REF(String.class);


        private final Class[] argTypes;

        Type(Class ... args) {
            this.argTypes = args;
        }

        public Class[] getArgTypes() {
            return argTypes;
        }
    }
}
