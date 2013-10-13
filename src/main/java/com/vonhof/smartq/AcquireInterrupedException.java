package com.vonhof.smartq;


public class AcquireInterrupedException extends InterruptedException {

    public AcquireInterrupedException() {
    }

    public AcquireInterrupedException(String s) {
        super(s);
    }
}
