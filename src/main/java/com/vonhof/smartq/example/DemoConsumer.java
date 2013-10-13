package com.vonhof.smartq.example;


import com.vonhof.smartq.Task;
import com.vonhof.smartq.server.SmartQConsumer;
import com.vonhof.smartq.server.SmartQConsumerHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class DemoConsumer {

    private static final Logger logger = Logger.getLogger(DemoConsumer.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        PropertyConfigurator.configure("log4j.properties");
        int consumerCount = 15;

        for(int i = 0 ; i < consumerCount; i++) {
            new SmartQConsumer<Task>(DemoProducer.ADDRESS,new SmartQConsumerHandler<Task>() {
                @Override
                public void taskReceived(SmartQConsumer<Task> consumer, Task task) throws Exception {
                    try {
                        Thread.sleep(15000 + (int)(60000 * Math.random()));
                    } catch (InterruptedException e) {
                        return;
                    }
                    consumer.acknowledge(task.getId());
                }
            }).connect();
        }


    }
}
