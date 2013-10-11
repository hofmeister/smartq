package com.vonhof.smartq.example;


import com.vonhof.smartq.Task;
import com.vonhof.smartq.server.SmartQConsumer;
import com.vonhof.smartq.server.SmartQConsumerHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.Date;

public class DemoConsumer {

    private static final Logger logger = Logger.getLogger(DemoConsumer.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        PropertyConfigurator.configure("log4j.properties");

        final SmartQConsumer consumer = new SmartQConsumer<Task>(DemoProducer.ADDRESS,new SmartQConsumerHandler<Task>() {
            @Override
            public void taskReceived(SmartQConsumer<Task> consumer, Task task) throws Exception {
                System.out.println(new Date() + " - Task: " + task.getId());
                try {
                    Thread.sleep((int)(10000 * Math.random()));
                } catch (InterruptedException e) {
                    return;
                }
                consumer.acknowledge(task.getId());
            }
        });

        consumer.connect();

        Thread acquireRun = new Thread() {
            @Override
            public void run() {
                while(!interrupted()) {
                    try {
                        consumer.acquire();
                        //System.out.println(new Date() + " - Acquire called");
                    } catch (RuntimeException ex) {
                      //Ignore
                    } catch (InterruptedException e) {
                        return;
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        };

        acquireRun.start();
        acquireRun.join();
    }
}
