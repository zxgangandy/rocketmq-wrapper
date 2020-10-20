package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.RMProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.junit.Before;
import org.junit.Test;

public class ProducerTest {
    private RMProducer producer;

    @Before
    public void init() {
        producer = RMWrapper.with(RMProducer.class)
                .producerGroup("producer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test1").retryTimes(3)
                .transactionListener(new TxListener())
                .start();
    }
    
    @Test
    public void sendMsgSync() {
        try {
            SendResult sendResult = producer.sendMessage(new MessageBody().setContent("a"));
            System.out.println("sendMsgSync, sendResult=" +sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String argv[]) {
        RMProducer producer = RMWrapper.with(RMProducer.class)
                .producerGroup("producer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test").retryTimes(3)
                .transactionListener(new TxListener())
                .start();

        try {
            producer.sendTransactionMessage(new MessageBody().setContent("b"),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
