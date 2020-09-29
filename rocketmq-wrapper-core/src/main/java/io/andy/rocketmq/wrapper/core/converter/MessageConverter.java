package io.andy.rocketmq.wrapper.core.converter;

import io.andy.rocketmq.wrapper.core.exception.MessageConversionException;

public interface MessageConverter {

    byte[] toMessageBody(Object message) throws MessageConversionException;

    Object fromMessageBody(byte[] message, Class<?> clazz) throws MessageConversionException;

}
