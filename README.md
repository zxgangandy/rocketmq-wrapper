# rocketmq-wrapper

[![AUR](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg)](https://github.com/zxgangandy/pigeon-rpc/blob/master/LICENSE)
[![](https://img.shields.io/badge/Author-zxgangandy-orange.svg)](https://github.com/zxgangandy/pigeon-rpc)
[![](https://img.shields.io/badge/version-1.1.1-brightgreen.svg)](https://github.com/zxgangandy/pigeon-rpc)

## 简介
Rocketmq-wrapper是对rocketmq client library的二次封装，支持普通消息和事务消息的发送和处理。Rocketmq-wrapper能大大方便我们使用rocketmq client来来构建应用程序，而忽略一些细节上的事件。

- 支持同步消息发送
- 支持异步消息发送
- 支持事务消息发送

## 使用
  ### 引入library：
  
  ``` xml
  <dependency>
    <groupId>io.github.zxgangandy</groupId>
    <artifactId>rocketmq-wrapper-core</artifactId>
    <version>1.1.1</version>
  </dependency>
  ```
     
  ### 消息生产者例子：
  

  ``` java
  private RMProducer producer;
  
      @Before
      public void init() {
          producer = RMWrapper.with(RMProducer.class)
                  .producerGroup("producer-test")
                  .nameSrvAddr("127.0.0.1:9876")
                  .topic("test1").retryTimes(3)
                  .txListener(new TxListener())
                  .start();
      }
  
      //同步消息
      @Test
          public void sendMsgSync() {
              try {
                  SendResult sendResult = producer.sendSync("test", new MessageBody().setContent("a"));
                  System.out.println("sendMsgSync, sendResult=" +sendResult);
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
  
      //异步消息
      @Test
          public void sendMsgAsync() {
              try {
                  producer.sendAsync("test", new MessageBody().setContent("b"), new SendCallback() {
                      @Override
                      public void onSuccess(SendResult sendResult) {
                          System.out.println("sendMsgAsync, sendResult=" +sendResult);
                      }
      
                      @Override
                      public void onException(Throwable e) {
                          System.out.println("sendMsgAsync, e=" +e);
                      }
                  });
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
  
      //事务消息
      @Test
          public void sendTxMsg() {
              try {
                  SendResult sendResult = producer.sendTransactional("test", new MessageBody().setContent("c"), "d");
                  System.out.println("sendTxMsg, sendResult=" +sendResult);
              } catch (Exception e) {
                  e.printStackTrace();
              }
          }
  
  ```
  
- 事务消息需要实现TransactionListener接口，在使用rocketmq-wrapper的时候只需要继承AbstractTransactionListener即可；
  
### 消息发送端例子
  
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

### 自定义消息序列化工具

- 用户也可以根据自己的喜好和业务要求定制自己的消息序列化工具，只需要实现MessageConverter接口


### 消息幂等

### 向多个集群发送消息

### 消息动态topic消费

### 注意事项
