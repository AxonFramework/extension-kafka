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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Implementation of the {@link StreamableMessageSource} that reads messages from a Kafka topic using the provided
 * {@link Fetcher}. Will create new {@link Consumer} instances for every call of {@link #openStream(TrackingToken)}, for
 * which it will create a unique Consumer Group Id. The latter ensures that we can guarantee that each Consumer Group
 * receives all messages, so that the {@link org.axonframework.eventhandling.TrackingEventProcessor} and it's {@link
 * org.axonframework.eventhandling.async.SequencingPolicy} are in charge of partitioning the load instead of Kafka.
 *
 * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
 * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class StreamableKafkaMessageSource<K, V> implements StreamableMessageSource<TrackedEventMessage<?>> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final List<String> topics;
    private final String groupIdPrefix;
    private final Supplier<String> groupIdSuffixFactory;
    private final ConsumerFactory<K, V> consumerFactory;
    private final Fetcher<K, V, KafkaEventMessage> fetcher;
    private final KafkaMessageConverter<K, V> messageConverter;
    private final Supplier<Buffer<KafkaEventMessage>> bufferFactory;

    /**
     * Instantiate a Builder to be able to create a {@link StreamableKafkaMessageSource}.
     * <p>
     * The {@code topics} list is defaulted to single entry of {@code "Axon.Events"}, {@code groupIdPrefix} defaults to
     * {@code "Axon.Streamable.Consumer-"} and it's {@code groupIdSuffixFactory} to a {@link UUID#randomUUID()}
     * operation, the {@link KafkaMessageConverter} to a {@link DefaultKafkaMessageConverter} using the {@link
     * XStreamSerializer} and the {@code bufferFactory} the {@link SortedKafkaMessageBuffer} constructor. The {@link
     * ConsumerFactory} and {@link Fetcher} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     * @return a Builder to be able to create an {@link StreamableKafkaMessageSource}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Instantiate a {@link StreamableKafkaMessageSource} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link ConsumerFactory} and {@link Fetcher} are not {@code null}. An {@link
     * AxonConfigurationException} is thrown if any of them is not the case.
     *
     * @param builder the {@link Builder} used to instantiate a {@link StreamableKafkaMessageSource} instance
     */
    protected StreamableKafkaMessageSource(Builder<K, V> builder) {
        builder.validate();
        this.topics = Collections.unmodifiableList(builder.topics);
        this.groupIdPrefix = builder.groupIdPrefix;
        this.groupIdSuffixFactory = builder.groupIdSuffixFactory;
        this.consumerFactory = builder.consumerFactory;
        this.fetcher = builder.fetcher;
        this.messageConverter = builder.messageConverter;
        this.bufferFactory = builder.bufferFactory;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The stream is filled by polling {@link ConsumerRecords} from the specified {@code topic} with the {@link
     * Fetcher}. The provided {@code trackingToken} is required to be of type {@link KafkaTrackingToken}.
     */
    @Override
    public BlockingStream<TrackedEventMessage<?>> openStream(TrackingToken trackingToken) {
        KafkaTrackingToken token = KafkaTrackingToken.from(trackingToken);
        TrackingRecordConverter<K, V> recordConverter = new TrackingRecordConverter<>(messageConverter, token);

        String groupId = buildConsumerGroupId();
        logger.debug("Consumer Group Id [{}] will start consuming from topics [{}]", groupId, topics);
        Consumer<K, V> consumer = consumerFactory.createConsumer(groupId);
        consumer.subscribe(
                topics,
                new TrackingTokenConsumerRebalanceListener<>(consumer, recordConverter::currentToken)
        );

        Buffer<KafkaEventMessage> buffer = bufferFactory.get();
        Registration closeHandler = fetcher.poll(consumer, recordConverter, buffer::putAll);
        return new KafkaMessageStream(buffer, closeHandler);
    }

    private String buildConsumerGroupId() {
        return groupIdPrefix + groupIdSuffixFactory.get();
    }

    /**
     * Builder class to instantiate a {@link StreamableKafkaMessageSource}.
     * <p>
     * The {@code topics} list is defaulted to single entry of {@code "Axon.Events"}, {@code groupIdPrefix} defaults to
     * {@code "Axon.Streamable.Consumer-"} and it's {@code groupIdSuffixFactory} to a {@link UUID#randomUUID()}
     * operation, the {@link KafkaMessageConverter} to a {@link DefaultKafkaMessageConverter} using the {@link
     * XStreamSerializer} and the {@code bufferFactory} the {@link SortedKafkaMessageBuffer} constructor. The {@link
     * ConsumerFactory} and {@link Fetcher} are <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> the key of the {@link ConsumerRecords} to consume, fetch and convert
     * @param <V> the value type of {@link ConsumerRecords} to consume, fetch and convert
     */
    public static class Builder<K, V> {

        private List<String> topics = Collections.singletonList("Axon.Events");
        private String groupIdPrefix = "Axon.Streamable.Consumer-";
        private Supplier<String> groupIdSuffixFactory = () -> UUID.randomUUID().toString();
        private ConsumerFactory<K, V> consumerFactory;
        private Fetcher<K, V, KafkaEventMessage> fetcher;
        @SuppressWarnings("unchecked")
        private KafkaMessageConverter<K, V> messageConverter =
                (KafkaMessageConverter<K, V>) DefaultKafkaMessageConverter.builder().serializer(
                        XStreamSerializer.builder().build()
                ).build();
        private Supplier<Buffer<KafkaEventMessage>> bufferFactory = SortedKafkaMessageBuffer::new;

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
         * Sets the prefix of the Consumer {@code groupId} from which a {@link Consumer} should retrieve records from.
         * Defaults to {@code "Axon.Streamable.Consumer-"}.
         *
         * @param groupIdPrefix a {@link String} defining the prefix of  the Consumer Group id to which a {@link
         *                      Consumer} should retrieve records from
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> groupIdPrefix(String groupIdPrefix) {
            assertThat(groupIdPrefix, name -> Objects.nonNull(name) && !"".equals(name),
                       "The groupIdPrefix may not be null or empty");
            this.groupIdPrefix = groupIdPrefix;
            return this;
        }

        /**
         * Sets the factory that will provide the suffix of the Consumer {@code groupId} from which a {@link Consumer}
         * should retrieve records from
         *
         * @param groupIdSuffixFactory a {@link Supplier} of {@link String} providing the suffix of the Consumer {@code
         *                             groupId} from which a {@link Consumer} should retrieve records from
         * @return the current Builder instance, for fluent interfacing
         */
        @SuppressWarnings("WeakerAccess")
        public Builder<K, V> groupIdSuffixFactory(Supplier<String> groupIdSuffixFactory) {
            assertNonNull(groupIdSuffixFactory, "GroupIdSuffixFactory may not be null");
            this.groupIdSuffixFactory = groupIdSuffixFactory;
            return this;
        }

        /**
         * Sets the {@link ConsumerFactory} to be used by this {@link StreamableKafkaMessageSource} to create {@link
         * Consumer} instances with.
         *
         * @param consumerFactory a {@link ConsumerFactory} to be used by this {@link StreamableKafkaMessageSource} to
         *                        create {@link Consumer} instances with.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> consumerFactory(ConsumerFactory<K, V> consumerFactory) {
            assertNonNull(consumerFactory, "ConsumerFactory may not be null");
            this.consumerFactory = consumerFactory;
            return this;
        }

        /**
         * Instantiate a {@link DefaultConsumerFactory} with the provided {@code consumerConfiguration}. Used by this
         * {@link StreamableKafkaMessageSource} to create {@link Consumer} instances with.
         *
         * @param consumerConfiguration a {@link DefaultConsumerFactory} with the given {@code consumerConfiguration},
         *                              to be used by this {@link StreamableKafkaMessageSource} to create {@link
         *                              Consumer} instances with
         * @return the current Builder instance, for fluent interfacing
         */
        @SuppressWarnings("unused")
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
        public Builder<K, V> fetcher(Fetcher<K, V, KafkaEventMessage> fetcher) {
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
         * Sets the {@code bufferFactory} of type {@link Supplier} with a generic type {@link Buffer} with {@link
         * KafkaEventMessage}s. Used to create a buffer which will consume the converted Kafka {@link ConsumerRecords}.
         * Defaults to a {@link SortedKafkaMessageBuffer}.
         *
         * @param bufferFactory a {@link Supplier} to create a buffer for the Kafka records fetcher
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> bufferFactory(Supplier<Buffer<KafkaEventMessage>> bufferFactory) {
            assertNonNull(bufferFactory, "Buffer factory may not be null");
            this.bufferFactory = bufferFactory;
            return this;
        }

        /**
         * Initializes a {@link StreamableKafkaMessageSource} as specified through this Builder.
         *
         * @return a {@link StreamableKafkaMessageSource} as specified through this Builder
         */
        public StreamableKafkaMessageSource<K, V> build() {
            return new StreamableKafkaMessageSource<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(consumerFactory, "The ConsumerFactory is a hard requirement and should be provided");
            assertNonNull(fetcher, "The Fetcher is a hard requirement and should be provided");
        }
    }
}
