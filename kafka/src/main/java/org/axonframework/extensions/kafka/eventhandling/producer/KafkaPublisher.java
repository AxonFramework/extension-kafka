/*
 * Copyright (c) 2010-2022. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.producer;

import com.thoughtworks.xstream.XStream;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.lifecycle.Lifecycle;
import org.axonframework.lifecycle.Phase;
import org.axonframework.messaging.EventPublicationFailedException;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.monitoring.MessageMonitor.MonitorCallback;
import org.axonframework.monitoring.NoOpMessageMonitor;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Publisher implementation that uses Kafka Message Broker to dispatch event messages. All outgoing messages are sent to
 * a configured topics.
 * <p>
 * This terminal does not dispatch Events internally, as it relies on each event processor to listen to it's own Kafka
 * Topic.
 * </p>
 *
 * @param <K> a generic type for the key of the {@link ProducerFactory}, {@link Producer} and {@link
 *            KafkaMessageConverter}
 * @param <V> a generic type for the value of the {@link ProducerFactory}, {@link Producer} and {@link
 *            KafkaMessageConverter}
 * @author Nakul Mishra
 * @author Simon Zambrovski
 * @author Lars Bilger
 * @since 4.0
 */
public class KafkaPublisher<K, V> implements Lifecycle {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    private static final String DEFAULT_TOPIC = "Axon.Events";

    private final ProducerFactory<K, V> producerFactory;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final MessageMonitor<? super EventMessage<?>> messageMonitor;
    private final TopicResolver topicResolver;
    private final long publisherAckTimeout;

    /**
     * Instantiate a {@link KafkaPublisher} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that {@link ProducerFactory} is not {@code null}, and will throw an {@link
     * AxonConfigurationException} if it is.
     *
     * @param builder the {@link Builder} used to instantiate a {@link KafkaPublisher} instance
     */
    protected KafkaPublisher(Builder<K, V> builder) {
        builder.validate();
        this.producerFactory = builder.producerFactory;
        this.messageConverter = builder.messageConverter;
        this.messageMonitor = builder.messageMonitor;
        this.topicResolver = builder.topicResolver;
        this.publisherAckTimeout = builder.publisherAckTimeout;
    }

    /**
     * Instantiate a Builder to be able to create a {@link KafkaPublisher}.
     * <p>
     * The {@link KafkaMessageConverter} is defaulted to a {@link DefaultKafkaMessageConverter}, the {@link
     * MessageMonitor} to a {@link NoOpMessageMonitor}, the {@code topic} to {code Axon.Events} and the {@code
     * publisherAckTimeout} to {@code 1000} milliseconds. The {@link ProducerFactory} is a
     * <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link ProducerFactory}, {@link Producer} and {@link
     *            KafkaMessageConverter}
     * @param <V> a generic type for the value of the {@link ProducerFactory}, {@link Producer} and {@link
     *            KafkaMessageConverter}
     * @return a Builder to be able to create a {@link KafkaPublisher}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Send {@code events} to the configured Kafka {@code topic}. It takes the current Unit of Work into account when
     * available.
     * <p>
     * If {@link ProducerFactory} is configured to use:
     * <ul>
     * <li>Transactions: use kafka transactions for publishing events.</li>
     * <li>Ack: send messages and wait for acknowledgement from Kafka. Acknowledgement timeout can be configured via
     * {@link KafkaPublisher.Builder#publisherAckTimeout(long)}).</li>
     * <li>None: fire and forget.</li>
     * </ul>
     *
     * @param event the events to publish on the Kafka broker.
     * @param <T>   the implementation of {@link EventMessage} send through this method
     */
    @SuppressWarnings("squid:S2095") //producer needs to be closed async, not within this method
    public <T extends EventMessage<?>> void send(T event) {
        logger.debug("Starting event producing process for [{}].", event.getPayloadType());
        Optional<String> topic = topicResolver.resolve(event);
        if (!topic.isPresent()) {
            logger.debug("Skip publishing event for [{}] since topicFunction returned empty.", event.getPayloadType());
            return;
        }
        UnitOfWork<?> uow = CurrentUnitOfWork.get();

        MonitorCallback monitorCallback = messageMonitor.onMessageIngested(event);
        Producer<K, V> producer = producerFactory.createProducer();
        ConfirmationMode confirmationMode = producerFactory.confirmationMode();

        if (confirmationMode.isTransactional()) {
            tryBeginTxn(producer);
        }

        // Sends event messages to Kafka and receive a future indicating the status.
        Future<RecordMetadata> publishStatus = producer.send(messageConverter.createKafkaMessage(event, topic.get()));

        uow.onPrepareCommit(u -> {
            if (confirmationMode.isTransactional()) {
                tryCommit(producer, monitorCallback);
            } else if (confirmationMode.isWaitForAck()) {
                waitForPublishAck(publishStatus, monitorCallback);
            }
            tryClose(producer);
        });

        uow.onRollback(u -> {
            if (confirmationMode.isTransactional()) {
                tryRollback(producer);
            }
            tryClose(producer);
        });
    }

    private void tryBeginTxn(Producer<?, ?> producer) {
        try {
            producer.beginTransaction();
        } catch (ProducerFencedException e) {
            logger.warn("Unable to begin transaction");
            throw new EventPublicationFailedException(
                    "Event publication failed, exception occurred while starting Kafka transaction.", e
            );
        }
    }

    private void tryCommit(Producer<?, ?> producer, MonitorCallback monitorCallback) {
        try {
            producer.commitTransaction();
            monitorCallback.reportSuccess();
        } catch (ProducerFencedException e) {
            logger.warn("Unable to commit transaction");
            monitorCallback.reportFailure(e);
            throw new EventPublicationFailedException(
                    "Event publication failed, exception occurred while committing Kafka transaction.", e
            );
        }
    }

    private void waitForPublishAck(Future<RecordMetadata> future, MonitorCallback monitorCallback) {
        long deadline = System.currentTimeMillis() + publisherAckTimeout;
        try {
            future.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            monitorCallback.reportSuccess();
        } catch (InterruptedException e) {
            monitorCallback.reportFailure(e);
            logger.warn("Encountered error while waiting for event publication.", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            monitorCallback.reportFailure(e);
            logger.warn("Encountered error while waiting for event publication.");
            throw new EventPublicationFailedException(
                    "Event publication failed, exception occurred while waiting for event publication.", e
            );
        }
    }

    private void tryRollback(Producer<?, ?> producer) {
        try {
            producer.abortTransaction();
        } catch (Exception e) {
            logger.warn("Unable to abort transaction.", e);
            // Not re-throwing exception, it's too late.
        }
    }

    private void tryClose(Producer<?, ?> producer) {
        try {
            producer.close();
        } catch (Exception e) {
            logger.debug("Unable to close producer.", e);
            // Not re-throwing exception, can't do anything.
        }
    }

    /**
     * Shuts down this component by calling {@link ProducerFactory#shutDown()} ensuring no new {@link Producer}
     * instances can be created. Upon shutdown of an application, this method is invoked in the
     * {@link Phase#INBOUND_EVENT_CONNECTORS} phase.
     */
    public void shutDown() {
        producerFactory.shutDown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerLifecycleHandlers(@Nonnull LifecycleRegistry lifecycle) {
        lifecycle.onShutdown(Phase.INBOUND_EVENT_CONNECTORS, this::shutDown);
    }

    /**
     * Builder class to instantiate a {@link KafkaPublisher}.
     * <p>
     * The {@link KafkaMessageConverter} is defaulted to a {@link DefaultKafkaMessageConverter}, the {@link
     * MessageMonitor} to a {@link NoOpMessageMonitor}, the {@code topic} to {code Axon.Events} and the {@code
     * publisherAckTimeout} to {@code 1000} milliseconds. The {@link ProducerFactory} is a
     * <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link ProducerFactory}, {@link Producer} and {@link
     *            KafkaMessageConverter}
     * @param <V> a generic type for the value of the {@link ProducerFactory}, {@link Producer} and {@link
     *            KafkaMessageConverter}
     */
    public static class Builder<K, V> {

        private ProducerFactory<K, V> producerFactory;
        private KafkaMessageConverter<K, V> messageConverter;
        private MessageMonitor<? super EventMessage<?>> messageMonitor = NoOpMessageMonitor.instance();
        private TopicResolver topicResolver = m -> Optional.of(DEFAULT_TOPIC);
        private long publisherAckTimeout = 1_000;
        private Supplier<Serializer> serializer;

        /**
         * Sets the {@link Serializer} used to serialize and deserialize messages. Defaults to a {@link
         * XStreamSerializer}.
         *
         * @param serializer a {@link Serializer} used to serialize and deserialize messages
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> serializer(Serializer serializer) {
            assertNonNull(serializer, "The Serializer may not be null");
            this.serializer = () -> serializer;
            return this;
        }

        /**
         * Sets the {@link ProducerFactory} which will instantiate {@link Producer} instances to publish {@link
         * EventMessage}s on the Kafka topic.
         *
         * @param producerFactory a {@link ProducerFactory} which will instantiate {@link Producer} instances to publish
         *                        {@link EventMessage}s on the Kafka topic
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> producerFactory(ProducerFactory<K, V> producerFactory) {
            assertNonNull(producerFactory, "ProducerFactory may not be null");
            this.producerFactory = producerFactory;
            return this;
        }

        /**
         * Sets the {@link KafkaMessageConverter} used to convert {@link EventMessage}s into Kafka messages. Defaults to
         * a {@link DefaultKafkaMessageConverter} using the {@link XStreamSerializer}.
         * <p>
         * Note that configuring a MessageConverter on the builder is mandatory if the value type is not {@code
         * byte[]}.
         *
         * @param messageConverter a {@link KafkaMessageConverter} used to convert {@link EventMessage}s into Kafka
         *                         messages
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> messageConverter(KafkaMessageConverter<K, V> messageConverter) {
            assertNonNull(messageConverter, "MessageConverter may not be null");
            this.messageConverter = messageConverter;
            return this;
        }

        /**
         * Sets the {@link MessageMonitor} of generic type {@link EventMessage} used the to monitor this Kafka
         * publisher. Defaults to a {@link NoOpMessageMonitor}.
         *
         * @param messageMonitor a {@link MessageMonitor} used to monitor this Kafka publisher
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> messageMonitor(MessageMonitor<? super EventMessage<?>> messageMonitor) {
            assertNonNull(messageMonitor, "MessageMonitor may not be null");
            this.messageMonitor = messageMonitor;
            return this;
        }

        /**
         * Set the Kafka {@code topic} to publish {@link EventMessage}s on. Defaults to {@code Axon.Events}.
         *
         * @param topic the Kafka {@code topic} to publish {@link EventMessage}s on
         * @return the current Builder instance, for fluent interfacing
         * @deprecated in through use of topic resolver
         */
        @Deprecated
        @SuppressWarnings("squid:S1133") //needs a major release to remove, since part of public API
        public Builder<K, V> topic(String topic) {
            assertThat(topic, name -> Objects.nonNull(name) && !"".equals(name), "The topic may not be null or empty");
            this.topicResolver = m -> Optional.of(topic);
            return this;
        }

        /**
         * Set the resolver to determine the Kafka {@code topic} to publish a certain {@link EventMessage} to.The {@code
         * EventMessage} is not published if the resolver returns an {@code Optional.empty()}. Defaults to always return
         * the set topic, or always return {@code Axon.Events}.
         *
         * @param topicResolver the Kafka {@code topic} to publish {@link EventMessage}s on
         * @return the current Builder instance, for fluent interfacing
         * @author Gerard Klijs
         * @since 4.6.0
         */
        public Builder<K, V> topicResolver(TopicResolver topicResolver) {
            assertNonNull(topicResolver, "The TopicResolver may not be null");
            this.topicResolver = topicResolver;
            return this;
        }

        /**
         * Sets the publisher acknowledge timeout in milliseconds specifying how long to wait for a publisher to
         * acknowledge a message has been sent. Defaults to {@code 1000} milliseconds.
         *
         * @param publisherAckTimeout a {@code long} specifying how long to wait for a publisher to acknowledge a
         *                            message has been sent
         * @return the current Builder instance, for fluent interfacing
         */
        @SuppressWarnings("WeakerAccess")
        public Builder<K, V> publisherAckTimeout(long publisherAckTimeout) {
            assertThat(
                    publisherAckTimeout, timeout -> timeout >= 0,
                    "The publisherAckTimeout should be a positive number or zero"
            );
            this.publisherAckTimeout = publisherAckTimeout;
            return this;
        }

        /**
         * Initializes a {@link KafkaPublisher} as specified through this Builder.
         *
         * @return a {@link KafkaPublisher} as specified through this Builder
         */
        public KafkaPublisher<K, V> build() {
            return new KafkaPublisher<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(producerFactory, "The ProducerFactory is a hard requirement and should be provided");
            if (serializer == null) {
                logger.warn(
                        "The default XStreamSerializer is used, whereas it is strongly recommended to configure"
                                + " the security context of the XStream instance.",
                        new AxonConfigurationException(
                                "A default XStreamSerializer is used, without specifying the security context"
                        )
                );
                serializer = () -> XStreamSerializer.builder()
                                                    .xStream(new XStream(new CompactDriver()))
                                                    .build();
            }
            if (messageConverter == null) {
                messageConverter = (KafkaMessageConverter<K, V>) DefaultKafkaMessageConverter.builder()
                                                                                             .serializer(serializer.get())
                                                                                             .build();
            }
        }
    }
}