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

package org.axonframework.extensions.kafka.eventhandling.consumer.subscribable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.messaging.SubscribableMessageSource;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * A {@link SubscribableMessageSource} implementation using Kafka's {@link Consumer} API to poll {@link
 * ConsumerRecords}. These records will be converted with a {@link KafkaMessageConverter} and given to the Event
 * Processor attached to the Consumer, added through the {@link #subscribe(java.util.function.Consumer)} method.
 * <p>
 * This approach allows the usage of Kafka's idea of load partitioning (through several Consumer instances in a Consumer
 * Groups) and resetting by adjusting a Consumer's offset. To share the load of a topic, several Event Processors
 * belonging to a group can be added through {@link #subscribe(java.util.function.Consumer)} operation, as each will get
 * it's own Consumer instances connected to the same Consumer Group.
 * <p>
 * If Axon's approach of segregating the event stream and replaying is desired, use the {@link
 * org.axonframework.extensions.kafka.eventhandling.consumer.tracking.StreamableKafkaMessageSource} instead.
 *
 * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
 * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
 * @author Steven van Beelen
 * @since 4.0
 */
public class SubscribableKafkaMessageSource<K, V> implements SubscribableMessageSource<EventMessage<?>> {

    private static final Logger logger = LoggerFactory.getLogger(SubscribableKafkaMessageSource.class);

    private final String topic;
    private final String groupId;
    private final ConsumerFactory<K, V> consumerFactory;
    private final Fetcher<EventMessage<?>, K, V> fetcher;
    private final KafkaMessageConverter<K, V> messageConverter;

    private final Set<java.util.function.Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArraySet<>();
    private final Map<java.util.function.Consumer<List<? extends EventMessage<?>>>, Runnable> consumerCloseHandlers = new HashMap<>();

    /**
     * Instantiate a Builder to be able to create a {@link SubscribableKafkaMessageSource}.
     * <p>
     * The {@code topic} is defaulted to {@code "Axon.Events"} and the {@link KafkaMessageConverter} to a {@link
     * DefaultKafkaMessageConverter} using the {@link XStreamSerializer}. The {@code groupId}, {@link ConsumerFactory}
     * and {@link Fetcher} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Instantiate a {@link SubscribableKafkaMessageSource} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@code groupId}, {@link ConsumerFactory} and {@link Fetcher} are not {@code null}. An {@link
     * AxonConfigurationException} is thrown if any of them is not the case.
     *
     * @param builder the {@link Builder} used to instantiate a {@link SubscribableKafkaMessageSource} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected SubscribableKafkaMessageSource(Builder<K, V> builder) {
        builder.validate();
        this.topic = builder.topic;
        this.groupId = builder.groupId;
        this.consumerFactory = builder.consumerFactory;
        this.fetcher = builder.fetcher;
        this.messageConverter = builder.messageConverter;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Any subscribed Event Processor will be placed in the same Consumer Group Id with it's own {@link Consumer}
     * instances polling records.
     */
    @Override
    public Registration subscribe(java.util.function.Consumer<List<? extends EventMessage<?>>> eventProcessor) {
        if (this.eventProcessors.add(eventProcessor)) {
            logger.debug("Event Processor [{}] subscribed successfully", eventProcessor);
        } else {
            logger.info("Event Processor [{}] not added. It was already subscribed", eventProcessor);
        }

        return () -> {
            if (eventProcessors.remove(eventProcessor)) {
                Runnable consumerCloser = consumerCloseHandlers.remove(eventProcessor);
                if (consumerCloser != null) {
                    consumerCloser.run();
                }

                logger.debug("Event Processor [{}] unsubscribed successfully", eventProcessor);
                return true;
            } else {
                logger.info("Event Processor [{}] not removed. It was already unsubscribed", eventProcessor);
                return false;
            }
        };
    }

    /**
     * Start polling the {@code topic} configured through {@link Builder#topic(String)} with a {@link Consumer} build by
     * the {@link ConsumerFactory} per subscribed Event Processor.
     */
    public void start() {
        start(Collections.singletonList(topic));
    }

    /**
     * Start polling the provided {@link List} of topics with a {@link Consumer} build by the {@link ConsumerFactory}
     * per subscribed Event Processor.
     *
     * @param topics a {@link List} of topics which a {@link Consumer} will start polling from
     */
    public void start(List<String> topics) {
        eventProcessors.forEach(eventProcessor -> {
            Consumer<K, V> consumer = consumerFactory.createConsumer(groupId);
            consumer.subscribe(topics);

            Runnable closeConsumer = fetcher.poll(
                    consumer,
                    consumerRecords -> StreamSupport.stream(consumerRecords.spliterator(), false)
                                                    .map(messageConverter::readKafkaMessage)
                                                    .filter(Optional::isPresent)
                                                    .map(Optional::get)
                                                    .collect(Collectors.toList()),
                    eventProcessor::accept
            );
            consumerCloseHandlers.put(eventProcessor, closeConsumer);
        });
    }


    /**
     * Close off this {@link SubscribableMessageSource} ensuring all used {@link Consumer}s per subscribed Event
     * Processor are closed.
     */
    public void close() {
        if (consumerCloseHandlers.isEmpty()) {
            logger.debug("No Event Processors have been subscribed who's Consumers should be closed");
            return;
        }
        consumerCloseHandlers.values().forEach(Runnable::run);
    }

    /**
     * Builder class to instantiate an {@link SubscribableKafkaMessageSource}.
     * <p>
     * The {@code topic} is defaulted to {@code "Axon.Events"} and the {@link KafkaMessageConverter} to a {@link
     * DefaultKafkaMessageConverter} using the {@link XStreamSerializer}. The {@code groupId}, {@link ConsumerFactory}
     * and {@link Fetcher} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static class Builder<K, V> {

        private String topic = "Axon.Events";
        private String groupId;
        private ConsumerFactory<K, V> consumerFactory;
        private Fetcher<EventMessage<?>, K, V> fetcher;
        @SuppressWarnings("unchecked")
        private KafkaMessageConverter<K, V> messageConverter =
                (KafkaMessageConverter<K, V>) DefaultKafkaMessageConverter.builder().serializer(
                        XStreamSerializer.builder().build()
                ).build();

        /**
         * Set the Kafka {@code topic} to read {@link org.axonframework.eventhandling.EventMessage}s from. Defaults to
         * {@code Axon.Events}.
         *
         * @param topic the Kafka {@code topic} to read {@link org.axonframework.eventhandling.EventMessage}s from
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> topic(String topic) {
            assertThat(topic, name -> Objects.nonNull(name) && !"".equals(name), "The topic may not be null or empty");
            this.topic = topic;
            return this;
        }

        /**
         * Sets the Consumer {@code groupId} from which a {@link Consumer} should retrieve records from
         *
         * @param groupId a {@link String} defining the Consumer Group id to which a {@link Consumer} should retrieve
         *                records from
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> groupId(String groupId) {
            assertThat(groupId, name -> Objects.nonNull(name) && !"".equals(name),
                       "The groupId may not be null or empty");
            this.groupId = groupId;
            return this;
        }

        /**
         * Sets the {@link ConsumerFactory} to be used by this {@link SubscribableKafkaMessageSource} implementation to
         * create {@link Consumer} instances.
         *
         * @param consumerFactory a {@link ConsumerFactory} to be used by this {@link SubscribableKafkaMessageSource}
         *                        implementation to create {@link Consumer} instances
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerFactory(ConsumerFactory<K, V> consumerFactory) {
            assertNonNull(consumerFactory, "ConsumerFactory may not be null");
            this.consumerFactory = consumerFactory;
            return this;
        }

        /**
         * Instantiate a {@link DefaultConsumerFactory} with the provided {@code consumerConfiguration}. Used by this
         * {@link SubscribableKafkaMessageSource} implementation to create {@link Consumer} instances.
         *
         * @param consumerConfiguration a {@link DefaultConsumerFactory} with the given {@code consumerConfiguration},
         *                              to be used by this {@link SubscribableKafkaMessageSource} implementation to
         *                              create {@link Consumer} instances
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerFactory(Map<String, Object> consumerConfiguration) {
            this.consumerFactory = new DefaultConsumerFactory<>(consumerConfiguration);
            return this;
        }

        /**
         * Sets the {@link Fetcher} used to poll, convert and consume {@link ConsumerRecords} with.
         *
         * @param fetcher the {@link Fetcher} used to poll, convert and consume {@link ConsumerRecords} with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> fetcher(Fetcher<EventMessage<?>, K, V> fetcher) {
            assertNonNull(fetcher, "Fetcher may not be null");
            this.fetcher = fetcher;
            return this;
        }

        /**
         * Sets the {@link KafkaMessageConverter} used to convert Kafka messages into {@link
         * org.axonframework.eventhandling.EventMessage}s. Defaults to a {@link DefaultKafkaMessageConverter} using the
         * {@link XStreamSerializer}.
         * <p>
         * Note that configuring a MessageConverter on the builder is mandatory if the value type is not {@code
         * byte[]}.
         *
         * @param messageConverter a {@link KafkaMessageConverter} used to convert Kafka messages into {@link
         *                         org.axonframework.eventhandling.EventMessage}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> messageConverter(KafkaMessageConverter<K, V> messageConverter) {
            assertNonNull(messageConverter, "MessageConverter may not be null");
            this.messageConverter = messageConverter;
            return this;
        }

        /**
         * Initializes a {@link SubscribableKafkaMessageSource} as specified through this Builder.
         *
         * @return a {@link SubscribableKafkaMessageSource} as specified through this Builder
         */
        public SubscribableKafkaMessageSource<K, V> build() {
            return new SubscribableKafkaMessageSource<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(groupId, "The Consumer Group Id is a hard requirement and should be provided");
            assertNonNull(consumerFactory, "The ConsumerFactory is a hard requirement and should be provided");
            assertNonNull(fetcher, "The Fetcher is a hard requirement and should be provided");
        }
    }
}
