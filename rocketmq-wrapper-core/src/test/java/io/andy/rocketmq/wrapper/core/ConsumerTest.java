package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.consumer.RMConsumer;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

public class ConsumerTest {

    @Test
    public void concurrentlyProcessor() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test")
                .concurrentlyProcessor(new ConcurrentlyMessageProcessor<MessageBody>() {
                    @Override
                    public ConsumeConcurrentlyStatus process(MessageExt rawMsg, MessageBody messageBody) {
                        System.out.println("concurrentlyProcessor, messageBody=" + messageBody);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                })
                .start();

        Thread.sleep(50000);
    }

}
