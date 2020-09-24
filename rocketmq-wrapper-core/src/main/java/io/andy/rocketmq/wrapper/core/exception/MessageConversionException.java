package io.andy.rocketmq.wrapper.core.exception;

public class MessageConversionException extends RuntimeException {

    public MessageConversionException(String message) {
        super(message);
    }

    public MessageConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
