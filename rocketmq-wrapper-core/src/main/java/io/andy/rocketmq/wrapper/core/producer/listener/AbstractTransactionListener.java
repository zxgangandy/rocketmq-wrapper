package io.andy.rocketmq.wrapper.core.producer.listener;

import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import static io.andy.rocketmq.wrapper.core.constant.Constants.MSG_BODY_CLASS;

public abstract class AbstractTransactionListener implements TransactionListener {

    protected volatile MessageConverter messageConverter;


    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return executeTransaction(msg, arg);
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Class<?> clazz;
        try {
            clazz = Class.forName(msg.getProperty(MSG_BODY_CLASS));
        } catch (Exception e) {
            return  LocalTransactionState.ROLLBACK_MESSAGE;
        }

        Object msgBody = messageConverter.fromMessageBody(msg.getBody(), clazz);
        return checkTransaction(msg, msgBody);
    }

    public abstract LocalTransactionState executeTransaction(Message msg, Object arg);

    public abstract LocalTransactionState checkTransaction(MessageExt msg, Object msgBody);
}
