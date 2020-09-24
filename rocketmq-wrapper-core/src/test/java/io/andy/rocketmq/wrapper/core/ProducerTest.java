package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.RMProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class ProducerTest {

    public static void main(String argv[]) {
        RMProducer producer = RMWrapper.with(RMProducer.class)
                .producerGroup("producer-test")
                .nameSrvAddr("127.0.0.1:9876")
                .topic("test").retryTimes(3)
                .transactionListener(new TransactionListener() {
                    @Override
                    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }

                    @Override
                    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }
                })
                .start();

        try {
            producer.sendTransactionMessage(new MessageBody().setTopic("topic"),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
