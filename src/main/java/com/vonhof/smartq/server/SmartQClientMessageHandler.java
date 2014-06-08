package com.vonhof.smartq.server;

import com.vonhof.smartq.Task;

public interface SmartQClientMessageHandler<U> {

    public void taskReceived(SmartQClient subscriber, Task<U> task) throws Exception;

}
