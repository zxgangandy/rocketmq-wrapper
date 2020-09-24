package io.andy.rocketmq.wrapper.core.converter;

import io.andy.rocketmq.wrapper.core.exception.MessageConversionException;

public interface MessageConverter {

    byte[] toMessageBody(Object var1) throws MessageConversionException;


    Object fromMessageBody(byte[] var1, Class<?> clazz) throws MessageConversionException;

}
