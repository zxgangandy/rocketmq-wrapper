package io.andy.rocketmq.wrapper.core.config;

public class BaseOption<T> extends Option<T> {

    protected BaseOption(String name, T defaultValue) {
        super(name, defaultValue);
    }


}
