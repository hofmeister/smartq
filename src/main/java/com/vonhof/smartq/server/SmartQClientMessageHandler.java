package com.vonhof.smartq.server;

import com.vonhof.smartq.Task;

public interface SmartQClientMessageHandler<T extends Task> {

    public void taskReceived(SmartQClient<T> subscriber, T task) throws Exception;

}
