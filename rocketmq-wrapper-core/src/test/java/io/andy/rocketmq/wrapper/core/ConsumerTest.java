package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.consumer.RMConsumer;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public class ConsumerTest {

    public static void main(String argv[]) {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test")
                .concurrentlyMessageProcessor(new ConcurrentlyMessageProcessor<String>() {
                    @Override
                    public ConsumeConcurrentlyStatus process(MessageExt msg, String messageBody) {
                        System.out.println("messageBody=" + messageBody);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                })
                .start();
    }
}
