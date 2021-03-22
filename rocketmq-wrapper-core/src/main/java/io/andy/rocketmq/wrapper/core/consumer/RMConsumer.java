package io.andy.rocketmq.wrapper.core.consumer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;

import io.andy.rocketmq.wrapper.core.consumer.listener.ConcurrentlyListener;
import io.andy.rocketmq.wrapper.core.consumer.listener.OrderlyListener;
import io.andy.rocketmq.wrapper.core.consumer.processor.ConcurrentlyProcessor;
import io.andy.rocketmq.wrapper.core.consumer.processor.OrderlyProcessor;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;

import java.lang.reflect.Field;
import java.util.Objects;

import static org.apache.rocketmq.common.consumer.ConsumeFromWhere.*;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;

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

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     */
    private long                           suspendCurrentQueueTimeMillis = 10000;

    private String                         nameSrvAddr;
    private String                         consumerGroup;

    private OrderlyProcessor               orderlyProcessor;
    private ConcurrentlyProcessor          concurrentlyProcessor;
    private MessageModel                   messageModel = MessageModel.CLUSTERING;
    private ConsumeFromWhere               consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
    private DefaultMQPushConsumer          pushConsumer;

    public RMConsumer() {
        init();
    }

    /**
     * @Description: 启动消费者
     * @date 2020-11-01
     *
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    @Override
    public RMConsumer start() {
        startConsumer();
        return this;
    }

    /**
     * @Description: 停止消费者
     * @date 2020-11-01
     *
     * @return: void
     */
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
     * @Description: 是否开启消息轨迹
     * @date 2020-11-01
     * @Param enableMsgTrace:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
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
     * @Description: 消费者消费组设置
     * @date 2020-11-10
     * @Param consumerGroup:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    public RMConsumer consumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    /** 消费者单序消息处理器设置
     * @Description:
     * @date 2020-11-10
     * @Param orderlyProcessor:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    public RMConsumer orderlyProcessor(OrderlyProcessor orderlyProcessor) {
        this.orderlyProcessor = orderlyProcessor;

        return this;
    }

    /**
     * @Description: 消费者并发消息处理器设置
     * @date 2020-11-10
     * @Param concurrentlyProcessor:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    public RMConsumer concurrentlyProcessor(ConcurrentlyProcessor concurrentlyProcessor) {
        this.concurrentlyProcessor = concurrentlyProcessor;

        return this;
    }

    /**
     * @Description: 消费者消息类型设置
     * @date 2020-11-10
     * @Param messageModel:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
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
     * @Description: 设置消费点
     * @date 2020-11-04
     * @Param consumeFromWhere:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    public RMConsumer consumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;

        return this;
    }

    /**
     * @Description: 设置重新消费的间隔时间
     * @date 2020-11-04
     * @Param consumeFromWhere:
     * @return: io.andy.rocketmq.wrapper.core.consumer.RMConsumer
     */
    public RMConsumer suspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;

        return this;
    }



    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic topic to subscribe.
     * Use default subscription, * expression,meaning subscribe all
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
     * Subscribe a topic to consuming subscription.
     *
     * @param topic topic to subscribe.
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     * if null or * expression,meaning subscribe all
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

    /**
     * @Description: 取消订阅topic消息
     * @date 2020-10-30
     * @Param topic:
     * @return: void
     */
    public void unsubscribe(String topic) {
        pushConsumer.unsubscribe(topic);
    }

    private void init() {
        pushConsumer = new DefaultMQPushConsumer();
    }

    private void startConsumer() {
        Objects.requireNonNull(consumerGroup);
        Objects.requireNonNull(nameSrvAddr);

        pushConsumer.setNamesrvAddr(nameSrvAddr);
        pushConsumer.setConsumerGroup(consumerGroup);
        pushConsumer.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);

        switch (consumeFromWhere) {
            case CONSUME_FROM_FIRST_OFFSET:
                pushConsumer.setConsumeFromWhere(CONSUME_FROM_FIRST_OFFSET);
                break;
            case CONSUME_FROM_LAST_OFFSET:
                pushConsumer.setConsumeFromWhere(CONSUME_FROM_LAST_OFFSET);
                break;
            case CONSUME_FROM_TIMESTAMP:
                pushConsumer.setConsumeFromWhere(CONSUME_FROM_TIMESTAMP);
                break;
            default:
                throw new IllegalArgumentException("Invalid consumeFromWhere");
        }

        switch (messageModel) {
            case BROADCASTING:
                pushConsumer.setMessageModel(BROADCASTING);
                break;
            case CLUSTERING:
                pushConsumer.setMessageModel(CLUSTERING);
                break;
            default:
                throw new IllegalArgumentException("Invalid messageModel");
        }

        final MessageConverter messageConverter = getRequiredMessageConverter();

        if (Objects.nonNull(orderlyProcessor)) {
            pushConsumer.registerMessageListener(
                    new OrderlyListener(orderlyProcessor, messageConverter));
        } else if (Objects.nonNull(concurrentlyProcessor)){
            pushConsumer.registerMessageListener(
                    new ConcurrentlyListener(concurrentlyProcessor, messageConverter));
        } else {
            throw new IllegalStateException("Consumer message listener not initialized yet!!");
        }

        if (enableMsgTrace) {
            enableMsgTrace();
        }

        try {
            pushConsumer.start();
        } catch (MQClientException e) {
            log.error("[消息消费者]--RMConsumer加载异常!e={}", e);
            throw new IllegalStateException("[消息消费者]--RMConsumer加载异常!", e);
        }

        log.info("[消息消费者]=>RMConsumer加载完成!");
    }

    private void enableMsgTrace() {
        try {
            AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup,
                    TraceDispatcher.Type.CONSUME, customizedTraceTopic, null);
            dispatcher.setHostConsumer(pushConsumer.getDefaultMQPushConsumerImpl());
            Class<DefaultMQPushConsumer> pushConsumerClass = (Class<DefaultMQPushConsumer>) pushConsumer.getClass();
            Field traceDispatcherField = pushConsumerClass.getDeclaredField("traceDispatcher");
            traceDispatcherField.setAccessible(true);
            traceDispatcherField.set(pushConsumer, dispatcher);
            pushConsumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                    new ConsumeMessageTraceHookImpl(dispatcher));
        } catch (Throwable e) {
            log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
        }
    }
}
