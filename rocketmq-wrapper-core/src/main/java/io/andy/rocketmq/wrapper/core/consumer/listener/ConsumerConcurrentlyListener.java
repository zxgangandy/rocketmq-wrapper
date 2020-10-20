package io.andy.rocketmq.wrapper.core.consumer.listener;


import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

import static io.andy.rocketmq.wrapper.core.utils.ReflectUtils.getMessageType;

/**
 * @author Andy
 * @desc 并发消息消费监听回调实现
 */
@Slf4j

public class ConsumerConcurrentlyListener implements MessageListenerConcurrently {

    private ConcurrentlyMessageProcessor messageProcessor;
    private MessageConverter             messageConverter;
    private Class<?>                     messageBodyClazz;

    public ConsumerConcurrentlyListener(ConcurrentlyMessageProcessor messageProcessor, MessageConverter messageConverter) {
        this.messageProcessor = messageProcessor;
        this.messageConverter = messageConverter;
        this.messageBodyClazz = getMessageType(this.messageProcessor, ConcurrentlyMessageProcessor.class);
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                int reconsumeTimes = msg.getReconsumeTimes();
                String msgId = msg.getMsgId();
                log.debug("msgId={},重复消费次数={}", msgId, reconsumeTimes);
                return handleMessage(msg, msgId);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            log.error("消息消费异常,e={}", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    /**
     * 处理收到的消息
     */
    private ConsumeConcurrentlyStatus handleMessage(MessageExt msg, String msgId) {
        Object message = messageConverter.fromMessageBody(msg.getBody(), messageBodyClazz);
        log.debug("msgId={}, 消费者接收到消息, message={}", msgId, message);

        return messageProcessor.process(msg, message);
    }




}
