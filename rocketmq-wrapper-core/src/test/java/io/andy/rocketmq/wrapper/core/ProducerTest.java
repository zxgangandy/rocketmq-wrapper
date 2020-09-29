package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.RMProducer;

public class ProducerTest {

    public static void main(String argv[]) {
        RMProducer producer = RMWrapper.with(RMProducer.class)
                .producerGroup("producer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test").retryTimes(3)
                .transactionListener(new TxListener())
                .start();

        try {
            producer.sendTransactionMessage(new MessageBody().setTopic("aaaa"),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
