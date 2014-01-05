package com.vonhof.smartq.example;


import com.vonhof.smartq.Task;
import com.vonhof.smartq.server.SmartQClient;
import com.vonhof.smartq.server.SmartQClientMessageHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class DemoSubscriber {

    private static final Logger logger = Logger.getLogger(DemoSubscriber.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        PropertyConfigurator.configure("log4j.properties");
        int subscriberCount = 15;

        for(int i = 0 ; i < subscriberCount; i++) {
            new SmartQClient<Task>(DemoPublisher.ADDRESS,new SmartQClientMessageHandler<Task>() {
                @Override
                public void taskReceived(SmartQClient<Task> subscriber, Task task) throws Exception {
                    try {
                        Thread.sleep(5000 + (int)(5000 * Math.random()));
                    } catch (InterruptedException e) {
                        return;
                    }
                    subscriber.acknowledge(task.getId());
                }
            }).connect();
        }


    }
}
