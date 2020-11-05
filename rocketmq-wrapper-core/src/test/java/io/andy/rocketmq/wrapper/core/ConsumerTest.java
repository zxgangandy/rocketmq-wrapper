package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.consumer.RMConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.junit.Test;

public class ConsumerTest {

    @Test
    public void concurrentlyProcessor() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .concurrentlyProcessor((messageBody) -> {
                    System.out.println("concurrentlyProcessor, messageBody=" + messageBody);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                })
                .start();

        Thread.sleep(50000);
    }

}
