package io.andy.rocketmq.wrapper.core.consumer.listener;


import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * @author Andy
 * @desc 并发消息消费监听回调实现
 */
@Slf4j

public class ConsumerConcurrentlyListener implements MessageListenerConcurrently {

    private ConcurrentlyMessageProcessor messageProcessor;

    public ConsumerConcurrentlyListener(ConcurrentlyMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                int reconsumeTimes = msg.getReconsumeTimes();
                String msgId = msg.getMsgId();
                log.info("msgId={},消费次数={}", msgId, reconsumeTimes);
                return handleMessage(msg, msgId);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            log.error("钱包扣款消费异常,e={}", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    /**
     * 处理收到的消息
     */
    private ConsumeConcurrentlyStatus handleMessage(MessageExt msg, String msgId) {
        String message = new String(msg.getBody());
        log.info("msgId={}, 消费者接收到消息, message={}", msgId, message);

        return messageProcessor.process(msg, message);
    }

}
