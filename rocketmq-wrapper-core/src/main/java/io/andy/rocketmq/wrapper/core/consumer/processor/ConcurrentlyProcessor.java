package io.andy.rocketmq.wrapper.core.consumer.processor;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public interface ConcurrentlyProcessor<T> {

    ConsumeConcurrentlyStatus process(T messageBody);
}
