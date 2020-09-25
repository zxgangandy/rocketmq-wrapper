package io.andy.rocketmq.wrapper.core.utils;

import org.springframework.aop.support.AopUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

public class ReflectUtils {

    public static Class getMessageType(Object messageProcessor, Class<?> clazz) {
        Type[] interfaces = AopUtils.getTargetClass(messageProcessor).getGenericInterfaces();
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), clazz)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        if (Objects.nonNull(actualTypeArguments) && actualTypeArguments.length > 0) {
                            return (Class) actualTypeArguments[0];
                        } else {
                            return Object.class;
                        }
                    }
                }
            }

            return String.class;
        } else {
            return Object.class;
        }
    }
}
