# rocketmq-wrapper

[![AUR](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg)](https://github.com/zxgangandy/pigeon-rpc/blob/master/LICENSE)
[![](https://img.shields.io/badge/Author-zxgangandy-orange.svg)](https://github.com/zxgangandy/pigeon-rpc)
[![](https://img.shields.io/badge/version-1.0.3-brightgreen.svg)](https://github.com/zxgangandy/pigeon-rpc)

## 简介
Rocketmq-wrapper是对rocketmq client library的二次封装，支持普通消息和事务消息的发送和处理。Rocketmq-wrapper能大大方便我们使用rocketmq client来来构建应用程序，而忽略一些细节上的事件。

- 支持同步消息发送
- 支持异步消息发送
- 支持事务消息发送

## 使用
  - 引入library：
  
  ``` xml
  <dependency>
    <groupId>io.github.zxgangandy</groupId>
    <artifactId>rocketmq-wrapper-core</artifactId>
    <version>1.0.3</version>
  </dependency>
  ```
     
  - 消息生产者例子：
  
  ``` java
  RMProducer producer = RMWrapper.with(RMProducer.class)
    .producerGroup("producer-test")
    .nameSrvAddr("127.0.0.1:9876")
    .topic("test").retryTimes(3)
    .transactionListener(new TransactionListener() {
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
  
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }).start();
  
  try {
    producer.sendTransactionMessage(new MessageBody().setTopic("topic"),null);
  } catch (Exception e) {
    e.printStackTrace();
  }
  
  ```
  - 消息发送端例子
  
  ``` java
  RMWrapper.with(RMConsumer.class)
      .consumerGroup("consumer-test")
      .nameSrvAddr("127.0.0.1:9876")
      .topic("test")
      .concurrentlyMessageProcessor(new ConcurrentlyMessageProcessor<MessageBody>() {
          @Override
          public ConsumeConcurrentlyStatus process(MessageExt rawMsg, MessageBody messageBody) {
             System.out.println("messageBody=" + messageBody);
             return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
      })
      .start();
    
  ```
