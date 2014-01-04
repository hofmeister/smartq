package com.vonhof.smartq;


import java.io.Serializable;

public class DefaultTaskResult implements Serializable{
    private final String message;
    private final boolean ok;

    public DefaultTaskResult(String message, boolean ok) {
        this.message = message;
        this.ok = ok;
    }

    public String getMessage() {
        return message;
    }

    public boolean isOk() {
        return ok;
    }
}
