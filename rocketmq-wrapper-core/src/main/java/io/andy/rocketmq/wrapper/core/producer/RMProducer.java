package io.andy.rocketmq.wrapper.core.producer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;
import io.andy.rocketmq.wrapper.core.config.Option;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.andy.rocketmq.wrapper.core.producer.listener.AbstractTransactionListener;
import lombok.extern.slf4j.Slf4j;
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
    private int                         retryTimes      = 2;
    private String                      nameSrvAddr;
    private String                      producerGroup;
    private String                      topic;

    private ExecutorService             executorService;
    private TransactionMQProducer       transactionMQProducer;
    private AbstractTransactionListener transactionListener;

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
     *  生产者topic设置
     */
    @Override
    public RMProducer topic(String topic) {
        this.topic = topic;

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
     *  生产者发送消息重试次数设置
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
    public SendResult sendMessage(Object req) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        return transactionMQProducer.send(message);
    }

    /**
     *  异步发送消息到broker
     */
    public void sendMessageAsync(Object req, SendCallback sendCallback) throws InterruptedException, RemotingException, MQClientException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        transactionMQProducer.send(message, sendCallback);
    }

    /**
     *  同步发送事务消息到broker
     */
    public  SendResult sendTransactionMessage(Object req, Object arg)  throws  MQClientException{
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

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

        transactionListener.setMessageConverter(messageConverter);

        transactionMQProducer = new TransactionMQProducer(producerGroup);
        transactionMQProducer.setNamesrvAddr(nameSrvAddr);
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(transactionListener);
        transactionMQProducer.setRetryTimesWhenSendFailed(retryTimes);
        //transactionMQProducer.setVipChannelEnabled(false);
        transactionMQProducer.setSendMsgTimeout(10000);
        try {
            transactionMQProducer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("启动[生产者]RMProducer异常", e);
        }
        log.info("启动[生产者]RMProducer, topic={}", topic);
    }


}
