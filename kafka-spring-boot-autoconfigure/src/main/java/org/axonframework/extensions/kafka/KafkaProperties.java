/*
 * Copyright (c) 2010-2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration properties for Axon for Apache Kafka.
 * <p>
 * Users should refer to Kafka documentation for complete descriptions of these properties.
 *
 * @author Gary Russell
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Nakul Mishra
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 4.0
 */
@ConfigurationProperties(prefix = "axon.kafka")
public class KafkaProperties {

    /**
     * Comma-delimited list of host:port pairs to use for establishing the initial connection to the Kafka cluster.
     */
    private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

    /**
     * Id to pass to the server when making requests; used for server-side logging.
     */
    private String clientId;

    /**
     * Default topic to which messages will be sent.
     */
    private String defaultTopic;

    /**
     * Additional properties, common to producers and consumers, used to configure the client.
     */
    private Map<String, String> properties = new HashMap<>();

    private final Publisher publisher = new Publisher();
    private final Producer producer = new Producer();

    private final Fetcher fetcher = new Fetcher();
    private final Consumer consumer = new Consumer();

    private final Ssl ssl = new Ssl();

    public List<String> getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @SuppressWarnings("unused")
    public void put(String key, String value) {
        this.properties.put(key, value);
    }

    public Publisher getPublisher() {
        return publisher;
    }

    public Producer getProducer() {
        return this.producer;
    }

    public Fetcher getFetcher() {
        return fetcher;
    }

    public Consumer getConsumer() {
        return this.consumer;
    }

    public Ssl getSsl() {
        return this.ssl;
    }

    @SuppressWarnings("Duplicates")
    private Map<String, Object> buildCommonProperties() {
        Map<String, Object> properties = new HashMap<>();
        if (this.bootstrapServers != null) {
            properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        }
        if (this.clientId != null) {
            properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, this.clientId);
        }
        if (this.ssl.getKeyPassword() != null) {
            properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.ssl.getKeyPassword());
        }
        if (this.ssl.getKeystoreLocation() != null) {
            properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getKeystoreLocation()));
        }
        if (this.ssl.getKeystorePassword() != null) {
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.ssl.getKeystorePassword());
        }
        if (this.ssl.getTruststoreLocation() != null) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getTruststoreLocation()));
        }
        if (this.ssl.getTruststorePassword() != null) {
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.ssl.getTruststorePassword());
        }
        if (!CollectionUtils.isEmpty(this.properties)) {
            properties.putAll(this.properties);
        }
        return properties;
    }

    /**
     * Create an initial map of consumer properties from the state of this instance.
     * <p>
     * This allows you to add additional properties, if necessary, and override the default {@code consumerFactory}
     * bean.
     *
     * @return the consumer properties initialized with the customizations defined on this instance
     */
    public Map<String, Object> buildConsumerProperties() {
        Map<String, Object> properties = buildCommonProperties();
        properties.putAll(this.consumer.buildProperties());
        return properties;
    }

    /**
     * Create an initial map of producer properties from the state of this instance.
     * <p>
     * This allows you to add additional properties, if necessary, and override the default {@code producerFactory}
     * bean.
     *
     * @return the producer properties initialized with the customizations defined on this instance
     */
    public Map<String, Object> buildProducerProperties() {
        Map<String, Object> properties = buildCommonProperties();
        properties.putAll(this.producer.buildProperties());
        return properties;
    }

    private static String resourceToPath(Resource resource) {
        try {
            return resource.getFile().getAbsolutePath();
        } catch (IOException ex) {
            throw new IllegalStateException("Resource '" + resource + "' must be on a file system", ex);
        }
    }

    /**
     * Configures the publisher used to provide Axon Event Messages as Kafka records to a Producer.
     */
    public static class Publisher {

        /**
         * The confirmation mode used when publishing messages. Defaults to {@link ConfirmationMode#NONE}.
         */
        private ConfirmationMode confirmationMode = ConfirmationMode.NONE;

        public ConfirmationMode getConfirmationMode() {
            return confirmationMode;
        }

        public void setConfirmationMode(ConfirmationMode confirmationMode) {
            this.confirmationMode = confirmationMode;
        }
    }

    /**
     * Configures the Producer which produces new records for a Kafka topic.
     */
    public static class Producer {

        /**
         * The SSL configuration for this Producer.
         */
        private final Ssl ssl = new Ssl();

        /**
         * Number of acknowledgments the producer requires the leader to have received before considering a request
         * complete.
         */
        private String acks;

        /**
         * Number of records to batch before sending.
         */
        private Integer batchSize;

        /**
         * Comma-delimited list of host:port pairs to use for establishing the initial connection to the Kafka cluster.
         */
        private List<String> bootstrapServers;

        /**
         * Total bytes of memory the producer can use to buffer records waiting to be sent to the server.
         */
        private Long bufferMemory;

        /**
         * Id to pass to the server when making requests; used for server-side logging.
         */
        private String clientId;

        /**
         * Compression type for all data generated by the producer.
         */
        private String compressionType;

        /**
         * When non empty, enables transaction support for producer.
         */
        private String transactionIdPrefix;

        /**
         * Serializer class for keys. Defaults to a {@link StringSerializer}.
         */
        private Class<?> keySerializer = StringSerializer.class;

        /**
         * Serializer class for values. Defaults to a {@link ByteArraySerializer}.
         */
        private Class<?> valueSerializer = ByteArraySerializer.class;

        /**
         * When greater than zero, enables retrying of failed sends.
         */
        private Integer retries;

        /**
         * Additional producer-specific properties used to configure the client.
         */
        private Map<String, String> properties = new HashMap<>();

        /**
         * Controls the mode of event processor responsible for sending messages to Kafka. Depending on this, different
         * error handling behaviours are taken in case of any errors during Kafka publishing.
         * <p>
         * Defaults to {@link EventProcessorMode#SUBSCRIBING}, using a {@link org.axonframework.eventhandling.SubscribingEventProcessor}
         * to publish events.
         */
        private EventProcessorMode eventProcessorMode = EventProcessorMode.SUBSCRIBING;

        public Ssl getSsl() {
            return this.ssl;
        }

        public String getAcks() {
            return this.acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public Integer getBatchSize() {
            return this.batchSize;
        }

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public List<String> getBootstrapServers() {
            return this.bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public Long getBufferMemory() {
            return this.bufferMemory;
        }

        public void setBufferMemory(Long bufferMemory) {
            this.bufferMemory = bufferMemory;
        }

        public String getClientId() {
            return this.clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getCompressionType() {
            return this.compressionType;
        }

        public void setCompressionType(String compressionType) {
            this.compressionType = compressionType;
        }

        public String getTransactionIdPrefix() {
            return transactionIdPrefix;
        }

        public void setTransactionIdPrefix(String transactionIdPrefix) {
            this.transactionIdPrefix = transactionIdPrefix;
        }

        public Class<?> getKeySerializer() {
            return this.keySerializer;
        }

        public void setKeySerializer(Class<?> keySerializer) {
            this.keySerializer = keySerializer;
        }

        public Class<?> getValueSerializer() {
            return this.valueSerializer;
        }

        public void setValueSerializer(Class<?> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public Integer getRetries() {
            return this.retries;
        }

        public void setRetries(Integer retries) {
            this.retries = retries;
        }

        public Map<String, String> getProperties() {
            return this.properties;
        }

        public EventProcessorMode getEventProcessorMode() {
            return eventProcessorMode;
        }

        public void setEventProcessorMode(EventProcessorMode eventProcessorMode) {
            this.eventProcessorMode = eventProcessorMode;
        }

        @SuppressWarnings({"Duplicates", "WeakerAccess"})
        public Map<String, Object> buildProperties() {
            Map<String, Object> properties = new HashMap<>();

            if (this.acks != null) {
                properties.put(ProducerConfig.ACKS_CONFIG, this.acks);
            }
            if (this.batchSize != null) {
                properties.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batchSize);
            }
            if (this.bootstrapServers != null) {
                properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            }
            if (this.bufferMemory != null) {
                properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.bufferMemory);
            }
            if (this.clientId != null) {
                properties.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientId);
            }
            if (this.compressionType != null) {
                properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.compressionType);
            }
            if (this.keySerializer != null) {
                properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer);
            }
            if (this.retries != null) {
                properties.put(ProducerConfig.RETRIES_CONFIG, this.retries);
            }
            if (this.ssl.getKeyPassword() != null) {
                properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.ssl.getKeyPassword());
            }
            if (this.ssl.getKeystoreLocation() != null) {
                properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getKeystoreLocation()));
            }
            if (this.ssl.getKeystorePassword() != null) {
                properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.ssl.getKeystorePassword());
            }
            if (this.ssl.getTruststoreLocation() != null) {
                properties.put(
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getTruststoreLocation())
                );
            }
            if (this.ssl.getTruststorePassword() != null) {
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.ssl.getTruststorePassword());
            }
            if (this.valueSerializer != null) {
                properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer);
            }
            if (!CollectionUtils.isEmpty(this.properties)) {
                properties.putAll(this.properties);
            }

            return properties;
        }
    }

    /**
     * Configures the Fetchers which fetches records from a Kafka Consumer to be put on an Event Stream.
     */
    public static class Fetcher {

        /**
         * The time, in milliseconds, spent waiting in poll if data is not available in the buffer. If 0, returns
         * immediately with any records that are available currently in the buffer, else returns empty. Must not be
         * negative and defaults to {@code 5000} milliseconds.
         *
         * @see KafkaConsumer#poll(Duration)
         */
        private long pollTimeout = 5_000;

        /**
         * Size of the buffer containing fetched Kafka records to be transferred over in to Axon Event Messages. Will
         * only be used for a {@link org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource}
         * instance. Defaults to {@code 10.000} records.
         */
        private int bufferSize = 10_000;

        public long getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(long pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }
    }

    /**
     * Configures the Consumer which consumes records from a Kafka topic.
     */
    public static class Consumer {

        /**
         * The SSL configuration for this Consumer.
         */
        private final Ssl ssl = new Ssl();

        /**
         * Frequency in milliseconds that the consumer offsets are auto-committed to Kafka if 'enable.auto.commit'
         * true.
         */
        private Integer autoCommitInterval;

        /**
         * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the
         * server.
         */
        private String autoOffsetReset;

        /**
         * Comma-delimited list of host:port pairs to use for establishing the initial connection to the Kafka cluster.
         */
        private List<String> bootstrapServers;

        /**
         * Id to pass to the server when making requests; used for server-side logging.
         */
        private String clientId;

        /**
         * If true the consumer's offset will be periodically committed in the background.
         */
        private Boolean enableAutoCommit;

        /**
         * Maximum amount of time in milliseconds the server will block before answering the fetch request if there
         * isn't sufficient data to immediately satisfy the requirement given by "fetch.min.bytes".
         */
        private Integer fetchMaxWait;

        /**
         * Minimum amount of data the server should return for a fetch request in bytes.
         */
        private Integer fetchMinSize;

        /**
         * Expected time in milliseconds between heartbeats to the consumer coordinator.
         */
        private Integer heartbeatInterval;

        /**
         * Deserializer class for keys. Defaults to a {@link StringDeserializer}.
         */
        private Class<?> keyDeserializer = StringDeserializer.class;

        /**
         * Deserializer class for values. Defaults to a {@link ByteArrayDeserializer}.
         */
        private Class<?> valueDeserializer = ByteArrayDeserializer.class;

        /**
         * Maximum number of records returned in a single call to poll().
         */
        private Integer maxPollRecords;

        /**
         * Additional consumer-specific properties used to configure the client.
         */
        private Map<String, String> properties = new HashMap<>();

        /**
         * Controls the default message source type which will be responsible for consuming records from Kafka and
         * providing them to an Event Processor. Defaults to {@link EventProcessorMode#TRACKING}, which will instantiate
         * a {@link org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource}.
         */
        private EventProcessorMode eventProcessorMode = EventProcessorMode.TRACKING;

        public Ssl getSsl() {
            return this.ssl;
        }

        public Integer getAutoCommitInterval() {
            return this.autoCommitInterval;
        }

        public void setAutoCommitInterval(Integer autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
        }

        public String getAutoOffsetReset() {
            return this.autoOffsetReset;
        }

        public void setAutoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }

        public List<String> getBootstrapServers() {
            return this.bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getClientId() {
            return this.clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public Boolean getEnableAutoCommit() {
            return this.enableAutoCommit;
        }

        public void setEnableAutoCommit(Boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }

        public Integer getFetchMaxWait() {
            return this.fetchMaxWait;
        }

        public void setFetchMaxWait(Integer fetchMaxWait) {
            this.fetchMaxWait = fetchMaxWait;
        }

        public Integer getFetchMinSize() {
            return this.fetchMinSize;
        }

        public void setFetchMinSize(Integer fetchMinSize) {
            this.fetchMinSize = fetchMinSize;
        }

        public Integer getHeartbeatInterval() {
            return this.heartbeatInterval;
        }

        public void setHeartbeatInterval(Integer heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
        }

        public Class<?> getKeyDeserializer() {
            return this.keyDeserializer;
        }

        public void setKeyDeserializer(Class<?> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public Class<?> getValueDeserializer() {
            return this.valueDeserializer;
        }

        public void setValueDeserializer(Class<?> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public Integer getMaxPollRecords() {
            return this.maxPollRecords;
        }

        public void setMaxPollRecords(Integer maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public Map<String, String> getProperties() {
            return this.properties;
        }

        public EventProcessorMode getEventProcessorMode() {
            return eventProcessorMode;
        }

        public void setEventProcessorMode(EventProcessorMode eventProcessorMode) {
            this.eventProcessorMode = eventProcessorMode;
        }

        @SuppressWarnings({"Duplicates", "WeakerAccess"})
        public Map<String, Object> buildProperties() {
            Map<String, Object> properties = new HashMap<>();

            if (this.autoCommitInterval != null) {
                properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, this.autoCommitInterval);
            }
            if (this.autoOffsetReset != null) {
                properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.autoOffsetReset);
            }
            if (this.bootstrapServers != null) {
                properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            }
            if (this.clientId != null) {
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
            }
            if (this.enableAutoCommit != null) {
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.enableAutoCommit);
            }
            if (this.fetchMaxWait != null) {
                properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, this.fetchMaxWait);
            }
            if (this.fetchMinSize != null) {
                properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, this.fetchMinSize);
            }
            if (this.heartbeatInterval != null) {
                properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, this.heartbeatInterval);
            }
            if (this.keyDeserializer != null) {
                properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer);
            }
            if (this.ssl.getKeyPassword() != null) {
                properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.ssl.getKeyPassword());
            }
            if (this.ssl.getKeystoreLocation() != null) {
                properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getKeystoreLocation()));
            }
            if (this.ssl.getKeystorePassword() != null) {
                properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.ssl.getKeystorePassword());
            }
            if (this.ssl.getTruststoreLocation() != null) {
                properties.put(
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resourceToPath(this.ssl.getTruststoreLocation())
                );
            }
            if (this.ssl.getTruststorePassword() != null) {
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.ssl.getTruststorePassword());
            }
            if (this.valueDeserializer != null) {
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer);
            }
            if (this.maxPollRecords != null) {
                properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords);
            }
            if (!CollectionUtils.isEmpty(this.properties)) {
                properties.putAll(this.properties);
            }

            return properties;
        }
    }

    /**
     * Modes for message production and consumption.
     */
    public enum EventProcessorMode {
        /**
         * For producing messages a {@link org.axonframework.eventhandling.SubscribingEventProcessor} will be used, that
         * will utilize Kafka's transactions for sending. A {@link org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource}
         * will be created for consuming messages.
         */
        SUBSCRIBING,
        /**
         * Use a {@link org.axonframework.eventhandling.TrackingEventProcessor} to publish messages. A {@link
         * org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource} will be
         * created for consuming messages.
         */
        TRACKING
    }

    /**
     * Configures SSL specifics for a Broker, Consumer and/or Producer.
     */
    public static class Ssl {

        /**
         * Password of the private key in the key store file.
         */
        private String keyPassword;

        /**
         * Location of the key store file.
         */
        private Resource keystoreLocation;

        /**
         * Store password for the key store file.
         */
        private String keystorePassword;

        /**
         * Location of the trust store file.
         */
        private Resource truststoreLocation;

        /**
         * Store password for the trust store file.
         */
        private String truststorePassword;

        public String getKeyPassword() {
            return this.keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public Resource getKeystoreLocation() {
            return this.keystoreLocation;
        }

        public void setKeystoreLocation(Resource keystoreLocation) {
            this.keystoreLocation = keystoreLocation;
        }

        public String getKeystorePassword() {
            return this.keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
        }

        public Resource getTruststoreLocation() {
            return this.truststoreLocation;
        }

        public void setTruststoreLocation(Resource truststoreLocation) {
            this.truststoreLocation = truststoreLocation;
        }

        public String getTruststorePassword() {
            return this.truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }
    }
}