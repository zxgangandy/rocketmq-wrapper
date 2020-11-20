package io.andy.rocketmq.wrapper.core.utils;

import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.andy.rocketmq.wrapper.core.producer.LocalTxState;
import io.andy.rocketmq.wrapper.core.producer.listener.MQTxListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import static io.andy.rocketmq.wrapper.core.constant.Constants.MSG_BODY_CLASS;

@Slf4j
public class MQUtils {
    public static TransactionListener convert(MQTxListener listener, MessageConverter messageConverter) {
        return new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return convertLocalTransactionState(listener.executeTransaction(arg));
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
                return convertLocalTransactionState(listener.checkTransaction(msgBody));
            }
        };
    }

    private static LocalTransactionState convertLocalTransactionState(LocalTxState state) {
        switch (state) {
            case UNKNOWN:
                return LocalTransactionState.UNKNOW;
            case COMMIT:
                return LocalTransactionState.COMMIT_MESSAGE;
            case ROLLBACK:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        log.warn("Failed to covert LocalTxState {}.", state);
        return LocalTransactionState.UNKNOW;
    }
}
