package io.andy.rocketmq.wrapper.core;


import io.andy.rocketmq.wrapper.core.config.Option;

public interface MQEndpoint {

    MQEndpoint start();

    <T> MQEndpoint config(Option<T> option, T value);

    <T> T putOptionGet(Option<T> option);

    MQEndpoint nameSrvAddr(String nameSrvAddr);

    void stop();
}
