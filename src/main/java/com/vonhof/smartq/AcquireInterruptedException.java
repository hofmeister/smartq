package com.vonhof.smartq;


public class AcquireInterruptedException extends InterruptedException {

    public AcquireInterruptedException() {
    }

    public AcquireInterruptedException(String s) {
        super(s);
    }
}
