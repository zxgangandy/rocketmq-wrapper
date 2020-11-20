package io.andy.rocketmq.wrapper.core.consumer.listener;


import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyProcessor;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

import static io.andy.rocketmq.wrapper.core.utils.ReflectUtils.getMessageType;


/**
 * @author Andy
 * @desc 顺序消息消费监听回调实现
 */
@Slf4j
public class OrderlyListener implements MessageListenerOrderly {
    private String                       charset = "UTF-8";
    private OrderlyProcessor             messageProcessor;
    private MessageConverter             messageConverter;
    private Class<?>                     messageBodyClazz;

    public OrderlyListener(OrderlyProcessor messageProcessor, MessageConverter messageConverter) {
        this.messageProcessor = messageProcessor;
        this.messageConverter = messageConverter;
        this.messageBodyClazz = getMessageType(this.messageProcessor, OrderlyProcessor.class);
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        try {
            for (MessageExt msg : msgs) {
                int reconsumeTimes = msg.getReconsumeTimes();
                String msgId = msg.getMsgId();
                log.debug("msgId={}, 重复消费次数={}", msgId, reconsumeTimes);
                return handleMessage(msg, msgId);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        } catch (Exception e) {
            log.error("消息消费异常, e={}", e);
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }

    /**
     * 处理收到的消息
     */
    private ConsumeOrderlyStatus handleMessage(MessageExt msg, String msgId) {
        if (Objects.equals(messageBodyClazz, MessageExt.class)) {
            return messageProcessor.process(msg);
        } else {
            if (Objects.equals(messageBodyClazz, String.class)) {
                String str = new String(msg.getBody(), Charset.forName(charset));
                return messageProcessor.process(str);
            } else {
                Object message = messageConverter.fromMessageBody(msg.getBody(), messageBodyClazz);
                log.debug("msgId={}, 消费者接收到顺序消息, message={}", msgId, message);

                return messageProcessor.process(message);
            }
        }
    }

}
