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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * A {@link SubscribableMessageSource} implementation using Kafka's {@link Consumer} API to poll {@link
 * ConsumerRecords}. These records will be converted with a {@link KafkaMessageConverter} and given to the Event
 * Processors subscribed to this source through the {@link #subscribe(java.util.function.Consumer)} method.
 * <p>
 * This approach allows the usage of Kafka's idea of load partitioning (through several Consumer instances in a Consumer
 * Groups) and resetting by adjusting a Consumer's offset. To share the load of a topic within a single application, the
 * {@link Builder#consumerCount(int)} can be increased to have multiple Consumer instances for this source, as each is
 * connected to the same Consumer Group. Running several instances of an application will essential have the same effect
 * when it comes to multi-threading event consumption.
 * <p>
 * If Axon's approach of segregating the event stream and replaying is desired, use the {@link
 * org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource} instead.
 *
 * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
 * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
 * @author Steven van Beelen
 * @since 4.0
 */
public class SubscribableKafkaMessageSource<K, V> implements SubscribableMessageSource<EventMessage<?>> {

    private static final Logger logger = LoggerFactory.getLogger(SubscribableKafkaMessageSource.class);

    private final List<String> topics;
    private final String groupId;
    private final ConsumerFactory<K, V> consumerFactory;
    private final Fetcher<K, V, EventMessage<?>> fetcher;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final boolean autoStart;
    private final int consumerCount;

    private final Set<java.util.function.Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArraySet<>();
    private final List<Registration> fetcherRegistrations = new CopyOnWriteArrayList<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    /**
     * Instantiate a Builder to be able to create a {@link SubscribableKafkaMessageSource}.
     * <p>
     * The {@code topics} list is defaulted to single entry of {@code "Axon.Events"} and the {@link
     * KafkaMessageConverter} to a {@link DefaultKafkaMessageConverter} using the {@link XStreamSerializer}. The {@code
     * groupId}, {@link ConsumerFactory} and {@link Fetcher} are <b>hard requirements</b> and as such should be
     * provided.
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
        this.topics = Collections.unmodifiableList(builder.topics);
        this.groupId = builder.groupId;
        this.consumerFactory = builder.consumerFactory;
        this.fetcher = builder.fetcher;
        this.messageConverter = builder.messageConverter;
        this.autoStart = builder.autoStart;
        this.consumerCount = builder.consumerCount;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Any subscribed Event Processor will be placed in the same Consumer Group, defined through the (mandatory) {@link
     * Builder#groupId(String)} method.
     */
    @Override
    public Registration subscribe(java.util.function.Consumer<List<? extends EventMessage<?>>> eventProcessor) {
        if (this.eventProcessors.add(eventProcessor)) {
            logger.debug("Event Processor [{}] subscribed successfully", eventProcessor);
        } else {
            logger.info("Event Processor [{}] not added. It was already subscribed", eventProcessor);
        }

        if (autoStart) {
            logger.info("Starting event consumption as auto start is enabled");
            start();
        }

        return () -> {
            if (eventProcessors.remove(eventProcessor)) {
                logger.debug("Event Processor [{}] unsubscribed successfully", eventProcessor);
                if (eventProcessors.isEmpty() && autoStart) {
                    logger.info("Closing event consumption as auto start is enabled");
                    close();
                }
                return true;
            } else {
                logger.info("Event Processor [{}] not removed. It was already unsubscribed", eventProcessor);
                return false;
            }
        };
    }

    /**
     * Start polling the {@code topics} configured through {@link Builder#topics(List)}/{@link Builder#addTopic(String)}
     * with a {@link Consumer} build by the {@link ConsumerFactory}.
     * <p>
     * This operation should be called <b>only</b> if all desired Event Processors have been subscribed (through the
     * {@link #subscribe(java.util.function.Consumer)} method).
     */
    public void start() {
        if (inProgress.getAndSet(true)) {
            return;
        }

        for (int consumerIndex = 0; consumerIndex < consumerCount; consumerIndex++) {
            Consumer<K, V> consumer = consumerFactory.createConsumer(groupId);
            consumer.subscribe(topics);

            Registration closeConsumer = fetcher.poll(
                    consumer,
                    consumerRecords -> StreamSupport.stream(consumerRecords.spliterator(), false)
                                                    .map(messageConverter::readKafkaMessage)
                                                    .filter(Optional::isPresent)
                                                    .map(Optional::get)
                                                    .collect(Collectors.toList()),
                    eventMessages -> eventProcessors.forEach(eventProcessor -> eventProcessor.accept(eventMessages))
            );
            fetcherRegistrations.add(closeConsumer);
        }
    }

    /**
     * Close off this {@link SubscribableMessageSource} ensuring all used {@link Consumer}s are closed.
     */
    public void close() {
        if (fetcherRegistrations.isEmpty()) {
            logger.debug("No Event Processors have been subscribed who's Consumers should be closed");
            return;
        }
        fetcherRegistrations.forEach(Registration::close);
        inProgress.set(false);
    }

    /**
     * Builder class to instantiate an {@link SubscribableKafkaMessageSource}.
     * <p>
     * The {@code topics} list is defaulted to single entry of {@code "Axon.Events"} and the {@link
     * KafkaMessageConverter} to a {@link DefaultKafkaMessageConverter} using the {@link XStreamSerializer}. The {@code
     * groupId}, {@link ConsumerFactory} and {@link Fetcher} are <b>hard requirements</b> and as such should be
     * provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static class Builder<K, V> {

        private List<String> topics = Collections.singletonList("Axon.Events");
        private String groupId;
        private ConsumerFactory<K, V> consumerFactory;
        private Fetcher<K, V, EventMessage<?>> fetcher;
        @SuppressWarnings("unchecked")
        private KafkaMessageConverter<K, V> messageConverter =
                (KafkaMessageConverter<K, V>) DefaultKafkaMessageConverter.builder().serializer(
                        XStreamSerializer.builder().build()
                ).build();
        private boolean autoStart = false;
        private int consumerCount = 1;

        /**
         * Set the Kafka {@code topics} to read {@link org.axonframework.eventhandling.EventMessage}s from. Defaults to
         * {@code Axon.Events}.
         *
         * @param topics the Kafka {@code topics} to read {@link org.axonframework.eventhandling.EventMessage}s from
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> topics(List<String> topics) {
            assertThat(topics, topicList -> Objects.nonNull(topicList) && !topicList.isEmpty(),
                       "The topics may not be null or empty");
            this.topics = new ArrayList<>(topics);
            return this;
        }

        /**
         * Add a Kafka {@code topic} to read {@link org.axonframework.eventhandling.EventMessage}s from.
         *
         * @param topic the Kafka {@code topic} to add to the list of topics
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> addTopic(String topic) {
            assertThat(topic, name -> Objects.nonNull(name) && !"".equals(name), "The topic may not be null or empty");
            topics.add(topic);
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
        public Builder<K, V> fetcher(Fetcher<K, V, EventMessage<?>> fetcher) {
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
         * Toggles on that after the first {@link #subscribe(java.util.function.Consumer)} operation, the {@link
         * #start()} method of this source will be called. Once all registered Consumer operations are removed, this
         * setting will close the source. By default this is behaviour is turned off.
         *
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> autoStart() {
            autoStart = true;
            return this;
        }

        /**
         * Sets the number of {@link Consumer} instances to create when this {@link SubscribableMessageSource} starts
         * consuming events. Default to {@code 1}.
         *
         * @param consumerCount the number of {@link Consumer} instances to create when this {@link
         *                      SubscribableMessageSource} starts consuming events
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerCount(int consumerCount) {
            assertThat(consumerCount, count -> count > 0, "The consumer count must be a positive, none zero number");
            this.consumerCount = consumerCount;
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
