package io.andy.rocketmq.wrapper.core;

public interface MQEndpoint {

    MQEndpoint start();

    MQEndpoint nameSrvAddr(String nameSrvAddr);

    void stop();
}
