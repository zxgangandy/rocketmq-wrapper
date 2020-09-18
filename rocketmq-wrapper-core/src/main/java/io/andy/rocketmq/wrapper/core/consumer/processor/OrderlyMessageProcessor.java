package io.andy.rocketmq.wrapper.core.consumer.processor;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

public interface OrderlyMessageProcessor {

    ConsumeOrderlyStatus process(MessageExt rawMsg, String messageBody);
}
