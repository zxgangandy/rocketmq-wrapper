package io.andy.rocketmq.wrapper.core.producer.listener;

import io.andy.rocketmq.wrapper.core.producer.LocalTxState;

public interface MQTxListener {
    LocalTxState executeTransaction(Object req);

    LocalTxState checkTransaction(Object body);
}
