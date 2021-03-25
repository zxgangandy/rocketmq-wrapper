package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.consumer.ConsumeFromWhere;
import io.andy.rocketmq.wrapper.core.consumer.MessageModel;
import io.andy.rocketmq.wrapper.core.consumer.RMConsumer;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyProcessor;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyProcessor;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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

    @Test
    public void orderlyProcessorWithEx() throws InterruptedException {
        Map m =new HashMap<>();
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .orderlyProcessor(new OrderlyProcessor<MessageExt>() {
                    @Override
                    public ConsumeOrderlyStatus process(MessageExt messageBody) {
                        System.out.println("orderlyProcessorWithFailed=>OrderlyProcessor, messageBody=" + messageBody);
                        System.out.println("map=" + m);
                        m.putIfAbsent(5, 5);

                        throw new NullPointerException("null exception");
                        //return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                })
                .start();

        Thread.sleep(50000000);
    }

    /**
     * @Description: 集群并行消费异常例子
     * @date 3/25/21
     *
     * @return: void
     */
    @Test
    public void concurrentlyProcessorWithSameGroupEx() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test0")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .messageModel(MessageModel.CLUSTERING)
                .concurrentlyProcessor(
                        new ConcurrentlyProcessor<MessageExt>() {
                            @Override
                            public ConsumeConcurrentlyStatus process(MessageExt messageBody) {
                                System.out.println("concurrentlyProcessor, cluster messageBody11=" +  messageBody);
                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                            }
                        }
                )
                .start();


        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test0")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .messageModel(MessageModel.CLUSTERING)
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .concurrentlyProcessor(new ConcurrentlyProcessor<MessageExt>() {
                    @Override
                    public ConsumeConcurrentlyStatus process(MessageExt messageBody) {
                        System.out.println("concurrentlyProcessor, cluster messageBody22=" +  messageBody);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                })
                .start();

        Thread.sleep(50000);
    }

    /**
     * @Description: 集群并行消费异常例子
     * @date 3/25/21
     *
     * @return: void
     */
    @Test
    public void concurrentlyProcessorWithDiffGroupOK() throws InterruptedException {
        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test0")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .messageModel(MessageModel.CLUSTERING)
                .concurrentlyProcessor(
                        new ConcurrentlyProcessor<MessageExt>() {
                            @Override
                            public ConsumeConcurrentlyStatus process(MessageExt messageBody) {
                                System.out.println("concurrentlyProcessor, cluster messageBody11=" +  messageBody);
                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                            }
                        }
                )
                .start();


        RMWrapper.with(RMConsumer.class)
                .consumerGroup("consumer-test1")
                .nameSrvAddr("127.0.0.1:9876")
                .subscribe("test")
                .messageModel(MessageModel.CLUSTERING)
                .consumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
                .concurrentlyProcessor(new ConcurrentlyProcessor<MessageExt>() {
                    @Override
                    public ConsumeConcurrentlyStatus process(MessageExt messageBody) {
                        System.out.println("concurrentlyProcessor, cluster messageBody22=" +  messageBody);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                })
                .start();

        Thread.sleep(50000);
    }
}
