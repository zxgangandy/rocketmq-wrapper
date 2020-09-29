package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.producer.listener.AbstractTransactionListener;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TxListener extends AbstractTransactionListener{

    @Override
    public LocalTransactionState executeTransaction(Message msg, Object arg) {
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkTransaction(MessageExt msg, Object msgBody) {
        return LocalTransactionState.COMMIT_MESSAGE;
    }

}
