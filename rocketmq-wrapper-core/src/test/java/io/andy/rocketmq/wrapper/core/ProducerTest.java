package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.RMProducer;
import org.apache.rocketmq.client.producer.SendCallback;
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
                .retryTimes(3)
                .transactionListener(new TxListener())
                .start();
    }

    @Test
    public void sendMsgSync() {
        try {
            SendResult sendResult = producer.sendMessage("test", new MessageBody().setContent("a"));
            System.out.println("sendMsgSync, sendResult=" +sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendMsgAsync() {
        try {
            producer.sendMessageAsync("test", new MessageBody().setContent("b"), new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("sendMsgAsync, sendResult=" +sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("sendMsgAsync, e=" +e);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendTxMsg() {
        try {
            SendResult sendResult = producer.sendTransactionMessage("test", new MessageBody().setContent("c"), "d");
            System.out.println("sendTxMsg, sendResult=" +sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
