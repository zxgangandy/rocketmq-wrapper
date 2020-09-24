package io.andy.rocketmq.wrapper.core.consumer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;

import io.andy.rocketmq.wrapper.core.config.Option;
import io.andy.rocketmq.wrapper.core.config.Options;
import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerConcurrentlyListener;
import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerOrderlyListener;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.netty.util.internal.ObjectUtil;
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
    private boolean                        orderly;
    private String                         nameSrvAddr;
    private String                         consumerGroup;
    private String                         topic;

    private Options                        options = new Options();
    private OrderlyMessageProcessor        orderlyProcessor;
    private ConcurrentlyMessageProcessor   concurrentlyProcessor;
    private MessageModel                   messageModel = MessageModel.CLUSTERING;
    private DefaultMQPushConsumer          pushConsumer;

    @Override
    public RMConsumer start() {
        init();
        return this;
    }

    @Override
    public <T> RMConsumer config(Option<T> option, T value) {
        ObjectUtil.checkNotNull(option, "option");
        this.options.option(option, value);
        return this;
    }

    @Override
    public <T> T putOptionGet(Option<T> option) {
        return options.option(option);
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

    /**
     *  消费者topic设置
     */
    @Override
    public RMConsumer topic(String topic) {
        this.topic = topic;

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
     *  消费者是否是单序消息设置
     */
    public RMConsumer orderly(boolean orderly) {
        this.orderly = orderly;

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
    public RMConsumer concurrentlyMessageProcessor(ConcurrentlyMessageProcessor concurrentlyProcessor) {
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

    private synchronized void init() {
        Objects.requireNonNull(consumerGroup);
        Objects.requireNonNull(nameSrvAddr);
        Objects.requireNonNull(topic);

        pushConsumer = new DefaultMQPushConsumer(consumerGroup);

        pushConsumer.setNamesrvAddr(nameSrvAddr);
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.setMessageModel(messageModel);

        final MessageConverter messageConverter = getRequiredMessageConverter();

        if (orderly) {
            Objects.requireNonNull(orderlyProcessor);
            pushConsumer.registerMessageListener(
                    new ConsumerOrderlyListener(orderlyProcessor, messageConverter));
        } else {
            Objects.requireNonNull(concurrentlyProcessor);
            pushConsumer.registerMessageListener(
                    new ConsumerConcurrentlyListener(concurrentlyProcessor, messageConverter));
        }

        try {
            pushConsumer.subscribe(topic, "*");
            pushConsumer.start();
        } catch (MQClientException e) {
            log.error("[消息消费者]--RMConsumer加载异常!e={}", e);
            throw new IllegalStateException("[消息消费者]--RMConsumer加载异常!", e);
        }

        log.info("[消息消费者]--RMConsumer加载完成!");
    }

}
