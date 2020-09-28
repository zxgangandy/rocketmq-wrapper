package io.andy.rocketmq.wrapper.core.producer.listener;

import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import org.apache.rocketmq.client.producer.TransactionListener;

public abstract class AbstractTransactionListener implements TransactionListener {



    protected volatile MessageConverter messageConverter;


    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

}
