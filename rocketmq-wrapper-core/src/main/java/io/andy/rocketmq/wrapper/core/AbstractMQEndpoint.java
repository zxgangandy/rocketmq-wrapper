package io.andy.rocketmq.wrapper.core;

import io.andy.rocketmq.wrapper.core.converter.FastJsonMessageConverter;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import org.apache.rocketmq.common.topic.TopicValidator;

public abstract class AbstractMQEndpoint implements MQEndpoint {
    protected volatile MessageConverter messageConverter = new FastJsonMessageConverter();
    protected String                    customizedTraceTopic = TopicValidator.RMQ_SYS_TRACE_TOPIC;

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    protected MessageConverter getRequiredMessageConverter() throws IllegalStateException {
        MessageConverter converter = this.getMessageConverter();
        if (converter == null) {
            throw new IllegalStateException("No 'messageConverter' specified. Check configuration.");
        } else {
            return converter;
        }
    }
}
