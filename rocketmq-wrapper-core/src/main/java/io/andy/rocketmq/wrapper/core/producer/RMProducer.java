package io.andy.rocketmq.wrapper.core.producer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.andy.rocketmq.wrapper.core.exception.MessageSendException;
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

import java.util.ArrayList;
import java.util.Collection;
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
    private TransactionMQProducer       producer;
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

        if (producer != null) {
            producer.shutdown();
            producer = null;
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
     *  同步发送消息到broker，采用默认的发送超时时间
     */
    public SendResult sendSync(String topic, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, EMPTY, req, producer.getSendMsgTimeout());
    }

    /**
     *  同步发送某个topic的消息到broker，自定义发送超时时间
     */
    public SendResult sendSync(String topic, Object req, long timeout)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, EMPTY, req, timeout);
    }

    /**
     *  同步发送某个topic和tags的消息到broker
     */
    public SendResult sendSync(String topic, String tags, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, tags,req, producer.getSendMsgTimeout());
    }

    /**
     *  同步发送某个topic和tags的消息到broker，自定义发送超时时间
     */
    public SendResult sendSync(String topic, String tags, Object req, long timeout)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, tags, req, timeout, 0);
    }

    /**
     *  同步发送某个topic的延迟消息到broker(delayLevel: 1~18)
     */
    public SendResult sendSyncDelay(String topic, Object req, int delayLevel)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, EMPTY, req, producer.getSendMsgTimeout(), delayLevel);
    }

    /**
     *  同步发送某个topic和tags的延迟消息到broker
     */
    public SendResult sendSyncDelay(String topic, String tags, Object req, int delayLevel)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, tags, req, producer.getSendMsgTimeout(), delayLevel);
    }

    /**
     *  同步发送某个topic和tags的延迟消息到broker，自定义发送超时时间
     */
    public SendResult sendSyncDelay(String topic, String tags, Object req, long timeout, int delayLevel)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        if (delayLevel > 0) {
            message.setDelayTimeLevel(delayLevel);
        }
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        return producer.send(message, timeout);
    }


    /**
     * send sync batch messages with default sending timeout.
     *
     * @param topic message topic
     * @param messages Collection of {@link java.lang.Object}
     * @return {@link SendResult}
     */
    public  SendResult sendBatchSync(String topic, Collection<Object> messages) {
        return sendBatchSync(topic, EMPTY, messages, producer.getSendMsgTimeout());
    }

    /**
     * send sync batch messages with tags and default sending timeout.
     *
     * @param topic message topic
     * @param tags message tags
     * @param messages Collection of {@link java.lang.Object}
     * @return {@link SendResult}
     */
    public  SendResult sendBatchSync(String topic, String tags, Collection<Object> messages) {
        return sendBatchSync(topic, tags, messages, producer.getSendMsgTimeout());
    }

    /**
     * send sync batch messages with tags and a given sending timeout.
     *
     * @param topic message topic
     * @param tags message tags
     * @param messages Collection of {@link java.lang.Object}
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    public  SendResult sendBatchSync(String topic, String tags, Collection<Object> messages, long timeout) {
        if (Objects.isNull(messages) || messages.size() == 0) {
            log.error("send sync with batch failed. topic:{}, messages is empty ", topic);
            throw new IllegalArgumentException("`messages` can not be empty");
        }

        try {
            long now = System.currentTimeMillis();
            Collection<Message> rmqMsgs = new ArrayList<>();
            for (Object msg : messages) {
                if (Objects.isNull(msg)) {
                    log.warn("Found a message empty in the batch, skip it");
                    continue;
                }
                byte[] messageBody = getRequiredMessageConverter().toMessageBody(msg);
                Message message = new Message(topic, tags, messageBody);
                rmqMsgs.add(message);
            }

            SendResult sendResult = producer.send(rmqMsgs, timeout);
            long costTime = System.currentTimeMillis() - now;
            if (log.isDebugEnabled()) {
                log.debug("send messages cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            }
            return sendResult;
        } catch (Exception e) {
            log.error("send sync with batch failed. topic:{}, messages.size:{} ", topic, messages.size());
            throw new MessageSendException(e.getMessage(), e);
        }
    }


    /**
     *  异步发送消息到broker
     */
    public void sendAsync(String topic, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        sendAsync(topic, EMPTY, req, sendCallback);
    }

    /**
     *  异步发送消息到broker
     */
    public void sendAsync(String topic, String tags, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        producer.send(message, sendCallback);
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

        return producer.sendMessageInTransaction(message, arg);
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

        producer = new TransactionMQProducer(producerGroup);
        producer.setNamesrvAddr(nameSrvAddr);
        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);
        producer.setRetryTimesWhenSendFailed(retryTimes);
        producer.setRetryTimesWhenSendAsyncFailed(retryTimes);
        producer.setUnitName(unitName);

        if (StringUtils.isNotEmpty(instanceName)) {
            producer.setInstanceName(instanceName);
        }

        //producer.setVipChannelEnabled(false);
        producer.setSendMsgTimeout(10000);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("启动[生产者]RMProducer异常", e);
        }
        log.info("启动[生产者]RMProducer成功");
    }


}
