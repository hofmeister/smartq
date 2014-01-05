package com.vonhof.smartq.pubsub;

import com.vonhof.smartq.Task;

public interface SmartQSubscriberHandler<T extends Task> {

    public void taskReceived(SmartQSubscriber<T> subscriber, T task) throws Exception;

}
