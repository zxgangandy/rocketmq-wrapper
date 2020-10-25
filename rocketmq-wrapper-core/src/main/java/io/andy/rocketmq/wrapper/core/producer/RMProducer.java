package io.andy.rocketmq.wrapper.core.producer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.andy.rocketmq.wrapper.core.producer.listener.AbstractTransactionListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.andy.rocketmq.wrapper.core.constant.Constants.MSG_BODY_CLASS;

@Slf4j
public class RMProducer  extends AbstractMQEndpoint {
    private static final String         EMPTY = StringUtils.EMPTY;
    private int                         retryTimes      = 2;
    private String                      nameSrvAddr;
    private String                      producerGroup;
    private String                      instanceName;
    private String                      unitName;

    private ExecutorService             executorService;
    private TransactionMQProducer       transactionMQProducer;
    private AbstractTransactionListener transactionListener;

    @Override
    public RMProducer start() {
        init();
        return this;
    }

    @Override
    public void stop() {
        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }

        if (transactionMQProducer != null) {
            transactionMQProducer.shutdown();
            transactionMQProducer = null;
        }
    }

    /**
     *  生产者name server地址设置
     */
    @Override
    public RMProducer nameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
        return this;
    }

    /**
     *  设置事务消息的监听器
     */
    public RMProducer transactionListener(AbstractTransactionListener transactionListener) {
        this.transactionListener = transactionListener;

        return this;
    }

    /**
     *  生产者组设置
     */
    public RMProducer producerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
        return this;
    }

    /**
     *  设置生产者unitName（可以实现一个jvm向不同集群发送消息）
     */
    public RMProducer unitName(String unitName) {
        this.unitName = unitName;
        return this;
    }

    /**
     *  设置生产者instanceName（可以实现一个jvm向不同集群发送消息）
     */
    public RMProducer instanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    /**
     *  生产者发送同步/异步消息重试次数设置
     */
    public RMProducer retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    /**
     *  消息转换器设置
     */
    public RMProducer messageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }

    /**
     *  同步发送消息到broker
     */
    public SendResult sendMessage(String topic, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendMessage(topic, EMPTY, req);
    }

    /**
     *  同步发送消息到broker
     */
    public SendResult sendMessage(String topic, String tags, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        return transactionMQProducer.send(message);
    }

    /**
     *  异步发送消息到broker
     */
    public void sendMessageAsync(String topic, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        sendMessageAsync(topic, EMPTY, req, sendCallback);
    }

    /**
     *  异步发送消息到broker
     */
    public void sendMessageAsync(String topic, String tags, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        transactionMQProducer.send(message, sendCallback);
    }

    /**
     *  同步发送事务消息到broker
     */
    public  SendResult sendTransactionMessage(String topic, Object req, Object arg)  throws  MQClientException{
       return sendTransactionMessage(topic, EMPTY, req, arg);
    }

    /**
     *  同步发送事务消息到broker
     */
    public  SendResult sendTransactionMessage(String topic, String tags, Object req, Object arg)  throws  MQClientException{
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        return transactionMQProducer.sendMessageInTransaction(message, arg);
    }

    private synchronized void init() {

        Objects.requireNonNull(producerGroup);
        Objects.requireNonNull(nameSrvAddr);
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

        transactionListener.setMessageConverter(messageConverter);

        transactionMQProducer = new TransactionMQProducer(producerGroup);
        transactionMQProducer.setNamesrvAddr(nameSrvAddr);
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(transactionListener);
        transactionMQProducer.setRetryTimesWhenSendFailed(retryTimes);
        transactionMQProducer.setRetryTimesWhenSendAsyncFailed(retryTimes);
        transactionMQProducer.setUnitName(unitName);

        if (StringUtils.isNotEmpty(instanceName)) {
            transactionMQProducer.setInstanceName(instanceName);
        }

        //transactionMQProducer.setVipChannelEnabled(false);
        transactionMQProducer.setSendMsgTimeout(10000);
        try {
            transactionMQProducer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("启动[生产者]RMProducer异常", e);
        }
        log.info("启动[生产者]RMProducer成功");
    }


}
