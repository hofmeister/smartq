package com.vonhof.smartq;


public interface QueueListener {

    public void onAcquire(Task t);

    public void onSubmit(Task t);

    public void onDone(Task t);
}
