package io.andy.rocketmq.wrapper.core.producer;

import com.alibaba.fastjson.JSON;
import io.andy.rocketmq.wrapper.core.MQEndpoint;
import io.andy.rocketmq.wrapper.core.config.Option;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RMProducer  implements MQEndpoint {
    private int                      retryTimes = 2;
    private String                   nameSrvAddr;
    private String                   producerGroup;
    private String                   topic;
    private ExecutorService          executorService;
    private TransactionMQProducer    transactionMQProducer;
    private TransactionListener      transactionListener;




    @Override
    public RMProducer start() {
        init();
        return this;
    }

    @Override
    public <T> RMProducer config(Option<T> option, T value) {
        return null;
    }

    @Override
    public <T> T putOptionGet(Option<T> option) {
        return null;
    }

    @Override
    public void stop() {
        executorService.shutdown();
        transactionMQProducer.shutdown();
        executorService = null;
        transactionMQProducer = null;
    }

    @Override
    public RMProducer nameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
        return this;
    }

    @Override
    public RMProducer topic(String topic) {
        this.topic = topic;

        return this;
    }

    public RMProducer transactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;

        return this;
    }

    public RMProducer producerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
        return this;
    }

    public RMProducer retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    /**
     *  同步发送消息到broker
     */
    public SendResult sendMessage(Object req) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        String msgBody = JSON.toJSONString(req);
        Message message = new Message(topic, msgBody.getBytes());
        return transactionMQProducer.send(message);
    }

    /**
     *  异步发送消息到broker
     */
    public void sendMessageAsync(Object req, SendCallback sendCallback) throws InterruptedException, RemotingException, MQClientException {
        String msgBody = JSON.toJSONString(req);
        Message message = new Message(topic, msgBody.getBytes());
        transactionMQProducer.send(message, sendCallback);
    }

    /**
     *  同步发送事务消息到broker
     */
    public SendResult sendTransactionMessage(Object req, Object arg)  throws  MQClientException{
        String msgBody = JSON.toJSONString(req);
        Message message = new Message(topic, msgBody.getBytes());
        return transactionMQProducer.sendMessageInTransaction(message, arg);
    }

    private synchronized void init() {

        Objects.requireNonNull(producerGroup);
        Objects.requireNonNull(nameSrvAddr);
        Objects.requireNonNull(topic);
        Objects.requireNonNull(transactionListener);

        // 初始化回查线程池
        executorService = new ThreadPoolExecutor(
                5,
                512,
                10000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(512),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName(producerGroup + "-check-thread");
                    return null;
                });

        transactionMQProducer = new TransactionMQProducer(producerGroup);
        transactionMQProducer.setNamesrvAddr(nameSrvAddr);
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(transactionListener);
        transactionMQProducer.setRetryTimesWhenSendFailed(retryTimes);
        //transactionMQProducer.setVipChannelEnabled(false);
        //transactionMQProducer.setSendMsgTimeout(10000);
        try {
            transactionMQProducer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("启动[生产者]RMProducer异常", e);
        }
        log.info("启动[生产者]RMProducer, topic={}", topic);
    }
}
