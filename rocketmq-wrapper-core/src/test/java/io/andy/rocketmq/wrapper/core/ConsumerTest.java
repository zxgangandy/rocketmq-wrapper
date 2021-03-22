package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.consumer.ConsumeFromWhere;
import io.andy.rocketmq.wrapper.core.consumer.RMConsumer;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyProcessor;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

public class ConsumerTest {

    @Test
    public void concurrentlyProcessor() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test1")
                .concurrentlyProcessor((messageBody) -> {
                    System.out.println("concurrentlyProcessor, messageBody=" + messageBody);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                })
                .start();

        Thread.sleep(50000);
    }

    @Test
    public void orderlyRawProcessor() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .orderlyProcessor(new OrderlyProcessor<MessageExt>() {
                    @Override
                    public ConsumeOrderlyStatus process(MessageExt messageBody) {
                        System.out.println("OrderlyProcessor, messageBody=" + messageBody);
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                })
                .start();

        Thread.sleep(50000000);
    }

    @Test
    public void orderlyProcessor() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .orderlyProcessor(new OrderlyProcessor<MessageBody>() {
                    @Override
                    public ConsumeOrderlyStatus process(MessageBody messageBody) {
                        System.out.println("OrderlyProcessor, messageBody=" + messageBody);
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                })
                .start();

        Thread.sleep(50000000);
    }

    @Test
    public void orderlyProcessorWithFailed() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .orderlyProcessor(new OrderlyProcessor<MessageExt>() {
                    @Override
                    public ConsumeOrderlyStatus process(MessageExt messageBody) {
                        System.out.println("orderlyProcessorWithFailed=>OrderlyProcessor, messageBody=" + messageBody);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                })
                .start();

        Thread.sleep(50000000);
    }

}
