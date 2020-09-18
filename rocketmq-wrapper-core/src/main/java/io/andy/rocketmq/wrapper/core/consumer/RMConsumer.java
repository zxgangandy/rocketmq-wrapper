package io.andy.rocketmq.wrapper.core.consumer;

import io.andy.rocketmq.wrapper.core.MQEndpoint;

import io.andy.rocketmq.wrapper.core.config.Option;
import io.andy.rocketmq.wrapper.core.config.Options;
import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerConcurrentlyListener;
import io.andy.rocketmq.wrapper.core.consumer.listener.ConsumerOrderlyListener;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyMessageProcessor;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyMessageProcessor;
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
public class RMConsumer implements MQEndpoint {
    private String nameSrvAddr;
    private String consumerGroup;
    private String topic;
    private boolean orderly;

    private Options options = new Options();

    private OrderlyMessageProcessor orderlyProcessor;
    private ConcurrentlyMessageProcessor concurrentlyProcessor;

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

    }

    @Override
    public RMConsumer nameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
        return this;
    }

    @Override
    public RMConsumer topic(String topic) {
        this.topic = topic;

        return this;
    }

    public RMConsumer consumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public RMConsumer orderly(boolean orderly) {
        this.orderly = orderly;

        return this;
    }

    public RMConsumer orderlyProcessor(OrderlyMessageProcessor orderlyProcessor) {
        this.orderlyProcessor = orderlyProcessor;

        return this;
    }

    public RMConsumer concurrentlyMessageProcessor(ConcurrentlyMessageProcessor concurrentlyProcessor) {
        this.concurrentlyProcessor = concurrentlyProcessor;

        return this;
    }

    private void init() {
        Objects.requireNonNull(consumerGroup);
        Objects.requireNonNull(nameSrvAddr);
        Objects.requireNonNull(topic);

        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(consumerGroup);

        pushConsumer.setNamesrvAddr(nameSrvAddr);
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.setMessageModel(MessageModel.CLUSTERING);

        if (orderly) {
            Objects.requireNonNull(orderlyProcessor);
            pushConsumer.registerMessageListener(new ConsumerOrderlyListener(orderlyProcessor));
        } else {
            Objects.requireNonNull(concurrentlyProcessor);
            pushConsumer.registerMessageListener(new ConsumerConcurrentlyListener(concurrentlyProcessor));
        }

        try {
            pushConsumer.subscribe(topic, "*");
            pushConsumer.start();
        } catch (MQClientException e) {
            log.error("[消息消费者]--RMConsumer加载异常!e={}", e);
            throw new RuntimeException("[订单支付消息消费者]--OrderPaidConsumer加载异常!", e);
        }

        log.info("[消息消费者]--RMConsumer加载完成!");
    }

}
