package com.vonhof.smartq.server;


import java.io.Serializable;

public class Error implements Serializable {
    private final String message;

    public Error(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
