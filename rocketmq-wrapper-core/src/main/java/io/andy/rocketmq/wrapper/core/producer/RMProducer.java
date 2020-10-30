package io.andy.rocketmq.wrapper.core.producer;

import io.andy.rocketmq.wrapper.core.AbstractMQEndpoint;
import io.andy.rocketmq.wrapper.core.converter.MessageConverter;
import io.andy.rocketmq.wrapper.core.exception.MessageSendException;
import io.andy.rocketmq.wrapper.core.producer.listener.AbstractTransactionListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.andy.rocketmq.wrapper.core.constant.Constants.MSG_BODY_CLASS;


@Slf4j
public class RMProducer  extends AbstractMQEndpoint {
    private static final String         EMPTY = StringUtils.EMPTY;
    private static final int            DEFAULT_SEND_MSG_TIMEOUT = 10000;
    private int                         retryTimes = 2;
    private int                         sendMsgTimeout = DEFAULT_SEND_MSG_TIMEOUT;

    private AtomicBoolean               started = new AtomicBoolean(false);
    private boolean                     enableMsgTrace;

    private String                      nameSrvAddr;
    private String                      producerGroup;
    private String                      instanceName;
    private String                      unitName;
    private String                      accessKey;
    private String                      secretKey;

    private ExecutorService             checkExecutorService;
    private DefaultMQProducer           producer;
    private AbstractTransactionListener transactionListener;
    private MessageQueueSelector        messageQueueSelector = new SelectMessageQueueByHash();

    /**
     * @Description: 启动生产者
     * @date 2020-10-27
     *
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    @Override
    public RMProducer start() {
        if (started.compareAndSet(false, true)) {
            init();

            try {
                producer.start();
            } catch (MQClientException e) {
                throw new RuntimeException("启动[生产者]RMProducer异常", e);
            }
            log.info("启动[生产者]RMProducer成功");
            return this;
        } else {
            throw new IllegalStateException("Producer: " + producerGroup + "started yet!!");
        }
    }

    /**
     * @Description: 停止生产者
     * @date 2020-10-27
     *
     * @return: void
     */
    @Override
    public void stop() {
        if (checkExecutorService != null) {
            checkExecutorService.shutdown();
            checkExecutorService = null;
        }

        if (producer != null) {
            producer.shutdown();
            producer = null;
        }
    }

    /**
     * @Description: 生产者name server地址设置
     * @date 2020-10-27
     * @Param nameSrvAddr:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    @Override
    public RMProducer nameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
        return this;
    }

    /**
     * @Description: 设置事务消息的监听器
     * @date 2020-10-27
     * @Param transactionListener:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer transactionListener(AbstractTransactionListener transactionListener) {
        this.transactionListener = transactionListener;

        return this;
    }

    /**
     * @Description: 设置生产组
     * @date 2020-10-27
     * @Param producerGroup:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer producerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
        return this;
    }

    /**
     * @Description: 设置生产者unitName（可以实现一个jvm向不同集群发送消息）
     * @date 2020-10-27
     * @Param unitName:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer unitName(String unitName) {
        this.unitName = unitName;
        return this;
    }

    /**
     * @Description: 设置生产者instanceName（可以实现一个jvm向不同集群发送消息）
     * @date 2020-10-27
     * @Param instanceName: 实例名称
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer instanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    /**
     * @Description: 设置ACL的accessKey
     * @date 2020-10-29
     * @Param accessKey:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer accessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    /**
     * @Description: 设置ACL的secretKey
     * @date 2020-10-29
     * @Param secretKey:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer secretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    /**
     * @Description: 是否开启消息轨迹
     * @date 2020-10-29
     * @Param enableMsgTrace:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer enableMsgTrace(boolean enableMsgTrace) {
        this.enableMsgTrace = enableMsgTrace;
        return this;
    }

    /**
     * @Description: 设置customizedTraceTopic
     * @date 2020-10-29
     * @Param customizedTraceTopic:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer customizedTraceTopic(String customizedTraceTopic) {
        this.customizedTraceTopic = customizedTraceTopic;
        return this;
    }


    /***
     * @Description: 生产者发送同步/异步消息重试次数设置
     * @date 2020-10-27
     * @Param retryTimes: 重试次数
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    /**
     * @Description: 设置生产者发送消息的超时时间
     * @date 2020-10-28
     * @Param sendMsgTimeout:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer sendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
        return this;
    }

    /**
     * @Description: 消息转换器设置
     * @date 2020-10-27
     * @Param messageConverter:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer messageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }

    /**
     * @Description: 设置事务消息的回查线程池
     * @date 2020-10-28
     * @Param executorService:
     * @return: io.andy.rocketmq.wrapper.core.producer.RMProducer
     */
    public RMProducer setCheckExecutorService(ExecutorService executorService) {
        this.checkExecutorService = executorService;
        return this;
    }

    /**
     * @Description: 同步发送消息到broker，采用默认的发送超时时间
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSync(String topic, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, EMPTY, req, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 同步发送某个topic的消息到broker，自定义发送超时时间
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @Param timeout:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSync(String topic, Object req, long timeout)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, EMPTY, req, timeout);
    }

    /**
     * @Description: 同步发送某个topic和tags的消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSync(String topic, String tags, Object req)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSync(topic, tags,req, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 同步发送某个topic和tags的消息到broker，自定义发送超时时间
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param timeout:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSync(String topic, String tags, Object req, long timeout)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, tags, req, timeout, 0);
    }

    /**
     * @Description: 同步发送某个topic的延迟消息到broker(delayLevel: 1~18)
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @Param delayLevel:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSyncDelay(String topic, Object req, int delayLevel)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, EMPTY, req, producer.getSendMsgTimeout(), delayLevel);
    }

    /**
     * @Description: 同步发送某个topic和tags的延迟消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param delayLevel:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendSyncDelay(String topic, String tags, Object req, int delayLevel)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return sendSyncDelay(topic, tags, req, producer.getSendMsgTimeout(), delayLevel);
    }

    /**
     * @Description: 同步发送某个topic和tags的延迟消息到broker，自定义发送超时时间
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param timeout:
     * @Param delayLevel:
     * @return: org.apache.rocketmq.client.producer.SendResult
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
     * @Description: send sync batch messages with default sending timeout.
     *
     * @param topic message topic
     * @param messages Collection of {@link java.lang.Object}
     * @return {@link SendResult}
     */
    public  SendResult sendBatchSync(String topic, Collection<Object> messages) {
        return sendBatchSync(topic, EMPTY, messages, producer.getSendMsgTimeout());
    }

    /**
     * @Description: send sync batch messages with tags and default sending timeout.
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
     * @Description: 异步发送消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @Param sendCallback:
     * @return: void
     */
    public void sendAsync(String topic, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        sendAsync(topic, EMPTY, req, sendCallback);
    }

    /**
     * @Description: 异步发送消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param sendCallback:
     * @return: void
     */
    public void sendAsync(String topic, String tags, Object req, SendCallback sendCallback)
            throws InterruptedException, RemotingException, MQClientException {
        sendAsync(topic, tags, req, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 异步发送消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @Param sendCallback:
     * @Param timeout:
     * @return: void
     */
    public void sendAsync(String topic, Object req, SendCallback sendCallback, long timeout)
            throws InterruptedException, RemotingException, MQClientException {
        sendAsync(topic, EMPTY, req, sendCallback, timeout);
    }

    /**
     * @Description: 异步发送消息到broker
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param sendCallback:
     * @Param timeout:
     * @return: void
     */
    public void sendAsync(String topic, String tags, Object req, SendCallback sendCallback, long timeout)
            throws InterruptedException, RemotingException, MQClientException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        producer.send(message, sendCallback, timeout);
    }

    /**
     * @Description: 同步发送顺序消息
     * @date 2020-10-27
     * @Param topic:
     * @Param req:
     * @Param key:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendOrderly(String topic, Object req, Object key)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendOrderly(topic, EMPTY, req, messageQueueSelector, key, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 同步发送顺序消息
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param key:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendOrderly(String topic, String tags, Object req, Object key)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendOrderly(topic, tags, req, messageQueueSelector, key, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 同步发送顺序消息
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param selector:
     * @Param key:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendOrderly(String topic, String tags, Object req, MessageQueueSelector selector, Object key)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return sendOrderly(topic, tags, req, selector, key, producer.getSendMsgTimeout());
    }

    /**
     * @Description: 同步发送顺序消息
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param key:
     * @Param timeout:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendOrderly(String topic, String tags, Object req, Object key, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException{
        return sendOrderly(topic, tags, req, messageQueueSelector, key, timeout);
    }

    /**
     * @Description: 同步发送顺序消息
     * @date 2020-10-27
     * @Param topic:
     * @Param tags:
     * @Param req:
     * @Param key: Argument to work along with message queue selector.（like product id, order id）
     * @Param arg:
     * @Param timeout:
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public SendResult sendOrderly(String topic, String tags, Object req, MessageQueueSelector selector, Object key, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        return producer.send(message, selector, key, timeout);
    }

    /**
     * @Description: 同步发送事务消息到broker
     * @date 2020-10-27
     * @Param topic: topic
     * @Param req:   消息body
     * @Param arg:   扩展参数
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public  SendResult sendTransactional(String topic, Object req)  throws  MQClientException{
        return sendTransactional(topic, EMPTY, req, null);
    }

    /**
     * @Description: 同步发送事务消息到broker
     * @date 2020-10-27
     * @Param topic: topic
     * @Param req:   消息body
     * @Param arg:   扩展参数
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public  SendResult sendTransactional(String topic, String tags, Object req)  throws  MQClientException{
        return sendTransactional(topic, tags, req, null);
    }

    /**
     * @Description: 同步发送事务消息到broker
     * @date 2020-10-27
     * @Param topic: topic
     * @Param req:   消息body
     * @Param arg:   扩展参数
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public  SendResult sendTransactional(String topic, Object req, Object arg)  throws  MQClientException{
       return sendTransactional(topic, EMPTY, req, arg);
    }

    /**
     * @Description: 同步发送事务消息到broker
     * @date 2020-10-27
     * @Param topic: topic
     * @Param tags:  消息的二级分类
     * @Param req:   消息body
     * @Param arg:   扩展参数
     * @return: org.apache.rocketmq.client.producer.SendResult
     */
    public  SendResult sendTransactional(String topic, String tags, Object req, Object arg)  throws  MQClientException{
        if (((TransactionMQProducer)producer).getTransactionListener() == null) {
            throw new IllegalStateException("The TransactionMQProducer does not exist TransactionListener");
        }

        byte[] messageBody = getRequiredMessageConverter().toMessageBody(req);
        Message message = new Message(topic, tags, messageBody);
        message.putUserProperty(MSG_BODY_CLASS, req.getClass().getName());

        return producer.sendMessageInTransaction(message, arg);
    }

    private void init() {
        Objects.requireNonNull(producerGroup);
        Objects.requireNonNull(nameSrvAddr);

        createDefaultMQProducer();

        producer.setNamesrvAddr(nameSrvAddr);
        producer.setRetryTimesWhenSendFailed(retryTimes);
        producer.setRetryTimesWhenSendAsyncFailed(retryTimes);
        producer.setUnitName(unitName);
        producer.setSendMsgTimeout(sendMsgTimeout);

        if (transactionListener != null) {
            initTransactionEnv();
        }

        if (StringUtils.isNotEmpty(instanceName)) {
            producer.setInstanceName(instanceName);
        }
    }

    private void initTransactionEnv() {
        transactionListener.setMessageConverter(messageConverter);
        ((TransactionMQProducer)producer).setTransactionListener(transactionListener);

        // 初始化回查线程池
        if (checkExecutorService == null) {
            checkExecutorService = getDefaultCheckExecutorService();
        }

        ((TransactionMQProducer)producer).setExecutorService(checkExecutorService);
    }

    private ExecutorService getDefaultCheckExecutorService() {
        return new ThreadPoolExecutor(
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
    }

    private boolean enabledAcl() {
        return !StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey);
    }


    private void createDefaultMQProducer() {
        boolean enabledAcl = enabledAcl();

        if (enabledAcl) {
            SessionCredentials credentials = new SessionCredentials(accessKey, secretKey);
            producer = new TransactionMQProducer(producerGroup, new AclClientRPCHook(credentials));
            producer.setVipChannelEnabled(false);
        } else {
            producer = new TransactionMQProducer(producerGroup);
        }

        enableMsgTrace(enableMsgTrace, enabledAcl, customizedTraceTopic);
    }


    private void enableMsgTrace(boolean enableMsgTrace, boolean enabledAcl, String customizedTraceTopic) {
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup,
                        TraceDispatcher.Type.PRODUCE, customizedTraceTopic,
                        enabledAcl ? new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)) : null);
                dispatcher.setHostProducer(producer.getDefaultMQProducerImpl());

                Field field = DefaultMQProducer.class.getDeclaredField("traceDispatcher");
                field.setAccessible(true);
                field.set(producer, dispatcher);
                producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageTraceHookImpl(dispatcher));
            } catch (Throwable e) {
                log.error("Message trace hook init failed ,maybe can't send message trace data");
            }
        }
    }

}
