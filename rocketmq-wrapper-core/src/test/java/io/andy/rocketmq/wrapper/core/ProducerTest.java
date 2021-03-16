package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.LocalTxState;
import io.andy.rocketmq.wrapper.core.producer.RMProducer;
import io.andy.rocketmq.wrapper.core.producer.listener.MQTxListener;
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
                .defaultTopicQueueNums(1)
                .retryTimes(3)
                .txListener(new MQTxListener() {
                    @Override
                    public LocalTxState executeTransaction(Object req) {
                        return LocalTxState.COMMIT;
                    }

                    @Override
                    public LocalTxState checkTransaction(Object body) {
                        return LocalTxState.COMMIT;
                    }
                })
                .start();
    }

    @Test
    public void sendMsgSync() {
        try {

            SendResult sendResult = producer.sendSync("test1", new MessageBody().setContent("a"));
            System.out.println("sendMsgSync, sendResult=" +sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendMsgAsync() {
        try {
            producer.sendAsync("test", new MessageBody().setContent("b"), new SendCallback() {
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
            SendResult sendResult = producer.sendTransactional("test", new MessageBody().setContent("c"), "d");
            System.out.println("sendTxMsg, sendResult=" +sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendOrderlyMsg() {
        try {

            for(int i=0; i<100; i++) {
                SendResult sendResult = producer.sendOrderly("test", new MessageBody().setContent("c"), "d");
                System.out.println("sendTxMsg, sendResult=" + sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
