package com.vonhof.smartq.server;

import com.vonhof.smartq.Task;

public interface SmartQConsumerHandler<T extends Task> {

    public void taskReceived(SmartQConsumer<T> consumer, T task) throws Exception;

}
