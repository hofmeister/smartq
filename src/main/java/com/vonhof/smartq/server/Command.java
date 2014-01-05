package com.vonhof.smartq.server;


import com.vonhof.smartq.Task;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

public class Command implements Serializable {
    private final Type type;
    private final Object[] args;

    public Command(Type type,Object ... args) {
        this.type = type;
        this.args = args;

        if (type.getArgTypes().length != args.length) {
            throw new IllegalArgumentException(String.format("Command %s requires %s arguments: %s", type, type.getArgTypes().length, type.getArgTypes()));
        }

        for(int i = 0; i < type.getArgTypes().length; i++) {
            Class argType = type.getArgTypes()[i];
            Object arg = args[i];
            if (!argType.isInstance(arg)) {
                throw new IllegalArgumentException(String.format("Command %s requires %s arguments: %s. Got %s",type,type.getArgTypes().length,type.getArgTypes(),args));
            }
        }
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
        SUBSCRIBE(),
        ACK(UUID.class),
        NACK(UUID.class, Boolean.class),
        RECOVER(Collection.class),
        ERROR(UUID.class),
        PUBLISH(Task.class);


        private final Class[] argTypes;

        Type(Class ... args) {
            this.argTypes = args;
        }

        public Class[] getArgTypes() {
            return argTypes;
        }
    }
}
