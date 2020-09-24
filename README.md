# rocketmq-wrapper
Easy to use for rocketmq



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
                  })
                  .start();
  
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
