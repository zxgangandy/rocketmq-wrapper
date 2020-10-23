package io.andy.rocketmq.wrapper.starter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rocketmq")
@Data
public class RocketMQProperties {
    /**
     * The name server for rocketMQ, formats: `host:port;host:port`.
     */
    private String nameServer;

    private final RocketMQProperties.Producer producer = new RocketMQProperties.Producer();
    private final RocketMQProperties.Consumer consumer = new RocketMQProperties.Consumer();


    @Data
    public static final class Producer {
        /**
         * Group name of producer.
         */
        private String group;

        /**
         * Millis of send message timeout.
         */
        private int sendMessageTimeout = 3000;

        /**
         * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
         */
        private int compressMessageBodyThreshold = 1024 * 4;

        /**
         * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
         * This may potentially cause message duplication which is up to application developers to resolve.
         */
        private int retryTimesWhenSendFailed = 2;

        /**
         * <p> Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
         * This may potentially cause message duplication which is up to application developers to resolve.
         */
        private int retryTimesWhenSendAsyncFailed = 2;

        /**
         * Indicate whether to retry another broker on sending failure internally.
         */
        private boolean retryNextServer = false;

        /**
         * Maximum allowed message size in bytes.
         */
        private int maxMessageSize = 1024 * 1024 * 4;

        /**
         * The property of "access-key".
         */
        private String accessKey;

        /**
         * The property of "secret-key".
         */
        private String secretKey;

        /**
         * Switch flag instance for message trace.
         */
        private boolean enableMsgTrace = true;
    }

    @Data
    public static final class Consumer {

    }
}
