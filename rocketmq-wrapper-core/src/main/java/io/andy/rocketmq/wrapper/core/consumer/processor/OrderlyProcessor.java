package io.andy.rocketmq.wrapper.core.consumer.processor;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public interface OrderlyProcessor<T> {

    ConsumeOrderlyStatus process(T messageBody);
}
