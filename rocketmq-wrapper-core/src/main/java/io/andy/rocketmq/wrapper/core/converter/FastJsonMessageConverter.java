package io.andy.rocketmq.wrapper.core.converter;

import com.alibaba.fastjson.JSON;
import io.andy.rocketmq.wrapper.core.exception.MessageConversionException;

public class FastJsonMessageConverter implements MessageConverter {

    @Override
    public byte[] toMessageBody(Object body) throws MessageConversionException {
        String msgBody = JSON.toJSONString(body);
        return msgBody.getBytes();
    }

    @Override
    public Object fromMessageBody(byte[] msgBody, Class<?> clazz) throws MessageConversionException {
        if (msgBody != null) {
            try {
                return JSON.parseObject(msgBody, clazz);
            } catch (Exception e) {
                throw new MessageConversionException("failed to parse Message content", e);
            }
        }
        return null;
    }
}
