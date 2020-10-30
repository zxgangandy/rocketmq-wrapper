package io.andy.rocketmq.wrapper.core.consumer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;

import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerConcurrentlyListener;
import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerOrderlyListener;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.Objects;

/**
 *
 * Consumer retry policy:
 *
 * 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
 *
 */
@Slf4j
public class RMConsumer extends AbstractMQEndpoint {
    private boolean                        enableMsgTrace;

    private String                         nameSrvAddr;
    private String                         consumerGroup;

    private OrderlyMessageProcessor        orderlyProcessor;
    private ConcurrentlyMessageProcessor   concurrentlyProcessor;
    private MessageModel                   messageModel = MessageModel.CLUSTERING;
    private DefaultMQPushConsumer          pushConsumer;

    public RMConsumer() {
        init();
    }

    @Override
    public RMConsumer start() {
        startConsumer();
        return this;
    }

    @Override
    public void stop() {
        if (pushConsumer != null) {
            pushConsumer.shutdown();
            pushConsumer = null;
        }
    }

    /**
     *  消费者name server设置
     */
    @Override
    public RMConsumer nameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
        return this;
    }

    public RMConsumer enableMsgTrace(boolean enableMsgTrace) {
        this.enableMsgTrace = enableMsgTrace;
        return this;
    }

    /**
     * @Description: 设置customizedTraceTopic
     * @date 2020-10-29
     * @Param customizedTraceTopic:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMConsumer customizedTraceTopic(String customizedTraceTopic) {
        this.customizedTraceTopic = customizedTraceTopic;
        return this;
    }

    /**
     *  消费者消费组设置
     */
    public RMConsumer consumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    /**
     *  消费者单序消息处理器设置
     */
    public RMConsumer orderlyProcessor(OrderlyMessageProcessor orderlyProcessor) {
        this.orderlyProcessor = orderlyProcessor;

        return this;
    }

    /**
     *  消费者并发消息处理器设置
     */
    public RMConsumer concurrentlyProcessor(ConcurrentlyMessageProcessor concurrentlyProcessor) {
        this.concurrentlyProcessor = concurrentlyProcessor;

        return this;
    }

    /**
     *  消费者消息类型设置
     */
    public RMConsumer messageModel(MessageModel messageModel) {
        this.messageModel = messageModel;

        return this;
    }

    /**
     *  消息转换器设置
     */
    public RMConsumer messageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;

        return this;
    }

    /**
     *  订阅消息
     */
    public RMConsumer subscribe(String topic) {
        try {
            pushConsumer.subscribe(topic, "*");
        } catch (MQClientException e) {
            log.error("RMConsumer订阅异常， e={}", e);
            throw new IllegalStateException("RMConsumer订阅异常", e);
        }
        return this;
    }

    /**
     * @Description: 取消订阅topic消息
     * @date 2020-10-30
     * @Param topic:
     * @return: void
     */
    public void unsubscribe(String topic) {
        pushConsumer.unsubscribe(topic);
    }

    /**
     * 订阅消息
     */
    public RMConsumer subscribe(String topic, String subExpression) {
        try {
            pushConsumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            log.error("RMConsumer订阅异常， e={}", e);
            throw new IllegalStateException("RMConsumer订阅异常", e);
        }
        return this;
    }

    private void init() {
        pushConsumer = new DefaultMQPushConsumer(consumerGroup, enableMsgTrace, customizedTraceTopic);
    }

    private void startConsumer() {
        Objects.requireNonNull(consumerGroup);
        Objects.requireNonNull(nameSrvAddr);

        pushConsumer.setNamesrvAddr(nameSrvAddr);
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.setMessageModel(messageModel);

        final MessageConverter messageConverter = getRequiredMessageConverter();

        if (Objects.nonNull(orderlyProcessor)) {
            pushConsumer.registerMessageListener(
                    new ConsumerOrderlyListener(orderlyProcessor, messageConverter));
        } else if (Objects.nonNull(concurrentlyProcessor)){
            pushConsumer.registerMessageListener(
                    new ConsumerConcurrentlyListener(concurrentlyProcessor, messageConverter));
        } else {
            throw new IllegalStateException("Consumer message listener not initialized yet!!");
        }

        try {
            pushConsumer.start();
        } catch (MQClientException e) {
            log.error("[消息消费者]--RMConsumer加载异常!e={}", e);
            throw new IllegalStateException("[消息消费者]--RMConsumer加载异常!", e);
        }

        log.info("[消息消费者]=>RMConsumer加载完成!");
    }
}
