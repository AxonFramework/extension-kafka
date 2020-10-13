/*
 * Copyright (c) 2010-2018. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.axonframework.common.AxonConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * The {@link ProducerFactory} implementation to produce a {@code singleton} shared {@link Producer} instance.
 * <p>
 * The {@link Producer} instance is freed from the external {@link Producer#close()} invocation with the internal
 * wrapper. The real {@link Producer#close()} is called on the target {@link Producer} during the {@link #shutDown()}.
 * <p>
 * Setting {@link Builder#confirmationMode(ConfirmationMode)} to transactional produces a transactional producer; in
 * which case, a cache of producers is maintained; closing the producer returns it to the cache. If cache is full the
 * producer will be closed through {@link KafkaProducer#close(Duration)} and evicted from cache.
 *
 * @author Nakul Mishra
 * @since 4.0
 */
public class DefaultProducerFactory<K, V> implements ProducerFactory<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducerFactory.class);

    private final Duration closeTimeout;
    private final BlockingQueue<PoolableProducer<K, V>> cache;
    private final Map<String, Object> configuration;
    private final ConfirmationMode confirmationMode;
    private final String transactionIdPrefix;

    private final AtomicInteger transactionIdSuffix;

    private volatile ShareableProducer<K, V> nonTransactionalProducer;

    /**
     * Instantiate a {@link DefaultProducerFactory} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@code configuration} is not {@code null}, and will throw an {@link
     * AxonConfigurationException} if it is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link DefaultProducerFactory} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected DefaultProducerFactory(Builder<K, V> builder) {
        builder.validate();
        this.closeTimeout = builder.closeTimeout;
        this.cache = new ArrayBlockingQueue<>(builder.producerCacheSize);
        this.configuration = builder.configuration;
        this.confirmationMode = builder.confirmationMode;
        this.transactionIdPrefix = builder.transactionIdPrefix;
        this.transactionIdSuffix = new AtomicInteger();
    }

    /**
     * Instantiate a Builder to be able to create a {@link DefaultProducerFactory}.
     * <p>
     * The {@code closeTimeout} is defaulted to a {@link Duration#ofSeconds(long)} of {@code 30}, the {@code
     * producerCacheSize} defaults to {@code 10} and the {@link ConfirmationMode} is defaulted to {@link
     * ConfirmationMode#NONE}. The {@code configuration} is a <b>hard requirement</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link Producer} this {@link ProducerFactory} will create
     * @param <V> a generic type for the value of the {@link Producer} this {@link ProducerFactory} will create
     * @return a Builder to be able to create a {@link DefaultProducerFactory}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    @Override
    public Producer<K, V> createProducer() {
        if (confirmationMode.isTransactional()) {
            return createTransactionalProducer();
        }

        if (this.nonTransactionalProducer == null) {
            synchronized (this) {
                if (this.nonTransactionalProducer == null) {
                    this.nonTransactionalProducer = new ShareableProducer<>(createKafkaProducer(configuration));
                }
            }
        }

        return this.nonTransactionalProducer;
    }

    @Override
    public ConfirmationMode confirmationMode() {
        return confirmationMode;
    }

    /**
     * Return an unmodifiable reference to the configuration map for this factory. Useful for cloning to make a similar
     * factory.
     *
     * @return a configuration {@link Map} used by this {@link ProducerFactory} to build {@link Producer}s
     */
    public Map<String, Object> configurationProperties() {
        return Collections.unmodifiableMap(configuration);
    }

    /**
     * The {@code transactionalIdPrefix} used to mark all {@link Producer} instances.
     *
     * @return the {@code transactionalIdPrefix} used to mark all {@link Producer} instances
     */
    public String transactionIdPrefix() {
        return transactionIdPrefix;
    }

    @Override
    public void shutDown() {
        ProducerDecorator<K, V> producer = this.nonTransactionalProducer;
        this.nonTransactionalProducer = null;
        if (producer != null) {
            producer.delegate.close(this.closeTimeout);
        }
        producer = this.cache.poll();
        while (producer != null) {
            try {
                producer.delegate.close(this.closeTimeout);
            } catch (Exception e) {
                logger.error("Exception closing producer", e);
            }
            producer = this.cache.poll();
        }
    }

    private Producer<K, V> createTransactionalProducer() {
        Producer<K, V> producer = this.cache.poll();
        if (producer != null) {
            return producer;
        }
        Map<String, Object> configs = new HashMap<>(this.configuration);
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    this.transactionIdPrefix + this.transactionIdSuffix.getAndIncrement());
        producer = new PoolableProducer<>(createKafkaProducer(configs), cache, closeTimeout);
        producer.initTransactions();
        return producer;
    }

    private Producer<K, V> createKafkaProducer(Map<String, Object> configs) {
        return new KafkaProducer<>(configs);
    }

    /**
     * Abstract base class to apply the decorator pattern to a Kafka {@link Producer}. Implements all methods in the
     * {@link Producer} interface by calling the wrapped delegate. Subclasses can override any of the methods to add
     * their specific behaviour.
     *
     * @param <K> record key type
     * @param <V> record value type
     */
    private abstract static class ProducerDecorator<K, V> implements Producer<K, V> {

        private final Producer<K, V> delegate;

        ProducerDecorator(Producer<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return this.delegate.send(record);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return this.delegate.send(record, callback);
        }

        @Override
        public void flush() {
            this.delegate.flush();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return this.delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return this.delegate.metrics();
        }

        @Override
        public void initTransactions() {
            this.delegate.initTransactions();
        }

        @Override
        public void beginTransaction() throws ProducerFencedException {
            this.delegate.beginTransaction();
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
                throws ProducerFencedException {
            this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             ConsumerGroupMetadata consumerGroupMetadata)
                throws ProducerFencedException {
            this.delegate.sendOffsetsToTransaction(offsets, consumerGroupMetadata);
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            this.delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() throws ProducerFencedException {
            this.delegate.abortTransaction();
        }

        @Override
        public void close() {
            this.delegate.close();
        }

        @Override
        public void close(Duration timeout) {
            this.delegate.close(timeout);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " [delegate=" + this.delegate + "]";
        }
    }

    /**
     * A decorator for a Kafka {@link Producer} that returns itself to an instance pool when {@link #close()} is called
     * instead of actually closing the wrapped {@link Producer}. If the pool is already full (i.e. has the configured
     * amount of idle producers), the wrapped producer is closed instead.
     *
     * @param <K> record key type
     * @param <V> record value type
     */
    private static final class PoolableProducer<K, V> extends ProducerDecorator<K, V> {

        private final BlockingQueue<PoolableProducer<K, V>> pool;
        private final Duration closeTimeout;

        PoolableProducer(Producer<K, V> delegate,
                         BlockingQueue<PoolableProducer<K, V>> pool,
                         Duration closeTimeout) {
            super(delegate);
            this.pool = pool;
            this.closeTimeout = closeTimeout;
        }

        @Override
        public void close() {
            close(closeTimeout);
        }

        @Override
        public void close(Duration timeout) {
            boolean isAdded = this.pool.offer(this);
            if (!isAdded) {
                super.close(timeout);
            }
        }
    }

    /**
     * A decorator for a Kafka {@link Producer} that ignores any calls to {@link #close()} so it can be reused and
     * closed by any number of clients.
     *
     * @param <K> record key type
     * @param <V> record value type
     */
    private static final class ShareableProducer<K, V> extends ProducerDecorator<K, V> {

        ShareableProducer(Producer<K, V> delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            // Do nothing
        }

        @Override
        public void close(Duration timeout) {
            // Do nothing
        }
    }

    /**
     * Builder class to instantiate a {@link DefaultProducerFactory}.
     * <p>
     * The {@code closeTimeout} is defaulted to a {@link Duration#ofSeconds(long)} of {@code 30}, the {@code
     * producerCacheSize} defaults to {@code 10} and the {@link ConfirmationMode} is defaulted to {@link
     * ConfirmationMode#NONE}. The {@code configuration} is a <b>hard requirement</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link Producer} this {@link ProducerFactory} will create
     * @param <V> a generic type for the value of the {@link Producer} this {@link ProducerFactory} will create
     */
    public static final class Builder<K, V> {

        private Duration closeTimeout = Duration.ofSeconds(30);
        private int producerCacheSize = 10;
        private Map<String, Object> configuration;
        private ConfirmationMode confirmationMode = ConfirmationMode.NONE;
        private String transactionIdPrefix;

        /**
         * Set the {@code closeTimeout} specifying how long to wait when {@link Producer#close(Duration)} is invoked.
         * Defaults to a {@link Duration#ofSeconds(long)} of {@code 30}.
         *
         * @param timeout      the time to wait before invoking {@link Producer#close(Duration)} in units of {@code
         *                     temporalUnit}.
         * @param temporalUnit a {@link TemporalUnit} determining how to interpret the {@code timeout} parameter
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> closeTimeout(int timeout, TemporalUnit temporalUnit) {
            assertNonNull(temporalUnit, "The temporalUnit may not be null");
            return closeTimeout(Duration.of(timeout, temporalUnit));
        }

        /**
         * Set the {@code closeTimeout} specifying how long to wait when {@link Producer#close(Duration)} is invoked.
         * Defaults to a {@link Duration#ofSeconds(long)} of {@code 30}.
         *
         * @param closeTimeout the {@link Duration} to wait before invoking {@link Producer#close(Duration)}
         * @return the current Builder instance, for fluent interfacing
         */
        @SuppressWarnings("WeakerAccess")
        public Builder<K, V> closeTimeout(Duration closeTimeout) {
            assertThat(
                    closeTimeout,
                    timeoutDuration -> !timeoutDuration.isNegative(),
                    "The closeTimeout should be a positive duration"
            );
            assertNonNull(closeTimeout, "The closeTimeout may not be null");
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * Sets the number of {@link Producer} instances to cache. Defaults to {@code 10}.
         * <p>
         * Will instantiate an {@link ArrayBlockingQueue} based on this number.
         *
         * @param producerCacheSize an {@code int} specifying the number of {@link Producer} instances to cache
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> producerCacheSize(int producerCacheSize) {
            assertThat(producerCacheSize, size -> size > 0, "The producerCacheSize should be a positive number");
            this.producerCacheSize = producerCacheSize;
            return this;
        }

        /**
         * Sets the {@code configuration} properties for creating {@link Producer} instances.
         *
         * @param configuration a {@link Map} of {@link String} to {@link Object} containing Kafka properties for
         *                      creating {@link Producer} instances
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> configuration(Map<String, Object> configuration) {
            assertNonNull(configuration, "The configuration may not be null");
            this.configuration = Collections.unmodifiableMap(new HashMap<>(configuration));
            return this;
        }

        /**
         * Sets the {@link ConfirmationMode} for producing {@link Producer} instances. Defaults to {@link
         * ConfirmationMode#NONE}.
         *
         * @param confirmationMode the {@link ConfirmationMode} for producing {@link Producer} instances
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> confirmationMode(ConfirmationMode confirmationMode) {
            assertNonNull(confirmationMode, "ConfirmationMode may not be null");
            this.confirmationMode = confirmationMode;
            return this;
        }

        /**
         * Sets the prefix to generate the {@code transactional.id} required for transactional {@link Producer}s.
         *
         * @param transactionIdPrefix a {@link String} specifying the prefix used to generate the {@code
         *                            transactional.id} required for transactional {@link Producer}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> transactionalIdPrefix(String transactionIdPrefix) {
            assertNonNull(transactionIdPrefix, "The transactionalIdPrefix may not be null");
            this.transactionIdPrefix = transactionIdPrefix;
            return this.confirmationMode(ConfirmationMode.TRANSACTIONAL);
        }

        /**
         * Initializes a {@link DefaultProducerFactory} as specified through this Builder.
         *
         * @return a {@link DefaultProducerFactory} as specified through this Builder
         */
        public DefaultProducerFactory<K, V> build() {
            return new DefaultProducerFactory<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(configuration, "The configuration is a hard requirement and should be provided");
        }
    }
}
