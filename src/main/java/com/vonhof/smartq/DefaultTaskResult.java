package com.vonhof.smartq;


import java.io.Serializable;

public class DefaultTaskResult implements Serializable{
    public final String message;
    public final boolean ok;

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
