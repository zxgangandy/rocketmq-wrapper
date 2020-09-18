package io.andy.rocketmq.wrapper.core.consumer.listener;


import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyMessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * @author Andy
 * @desc 顺序消息消费监听回调实现
 */
@Slf4j

public class ConsumerOrderlyListener implements MessageListenerOrderly {

    private OrderlyMessageProcessor messageProcessor;

    public ConsumerOrderlyListener(OrderlyMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                int reconsumeTimes = msg.getReconsumeTimes();
                String msgId = msg.getMsgId();
                log.info("msgId={}, 消费次数={}", msgId, reconsumeTimes);
                return handleMessage(msg, msgId);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        } catch (Exception e) {
            log.error("钱包扣款消费异常, e={}", e);
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }

    /**
     * 处理收到的消息
     */
    private ConsumeOrderlyStatus handleMessage(MessageExt msg, String msgId) {
        String message = new String(msg.getBody());
        log.info("msgId={}, 消费者接收到消息, message={}", msgId, message);

        return messageProcessor.process(msg, message);
    }

}
