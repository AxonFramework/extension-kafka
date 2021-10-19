/*
 * Copyright (c) 2010-2018. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.EventData;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.async.SequencingPolicy;
import org.axonframework.eventhandling.async.SequentialPerAggregatePolicy;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;
import static org.axonframework.messaging.Headers.*;

/**
 * Converts and {@link EventMessage} to a {@link ProducerRecord} Kafka message and from a {@link ConsumerRecord} Kafka
 * message back to an EventMessage (if possible).
 * <p>
 * During conversion meta data entries with the {@code 'axon-metadata-'} prefix are passed to the {@link Headers}. Other
 * message-specific attributes are added as metadata. The {@link EventMessage#getPayload()} is serialized using the
 * configured {@link Serializer} and passed as the Kafka record's body.
 * <p>
 * <p>
 * If an up-caster / up-caster chain is configured, this converter will pass the converted messages through the it.
 * Please note, that the since the message converter consumes records one-by-one, the up-casting functionality is
 * limited to one-to-one and one-to-many up-casters only.
 * </p>
 * This implementation will suffice in most cases.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class DefaultKafkaMessageConverter implements KafkaMessageConverter<String, byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConverter.class);

    private final Serializer serializer;
    private final SequencingPolicy<? super EventMessage<?>> sequencingPolicy;
    private final BiFunction<String, Object, RecordHeader> headerValueMapper;
    private final EventUpcasterChain upcasterChain;

    /**
     * Instantiate a {@link DefaultKafkaMessageConverter} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Serializer} is not {@code null} and will throw an {@link AxonConfigurationException}
     * if it is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link DefaultKafkaMessageConverter} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected DefaultKafkaMessageConverter(Builder builder) {
        builder.validate();
        this.serializer = builder.serializer;
        this.sequencingPolicy = builder.sequencingPolicy;
        this.headerValueMapper = builder.headerValueMapper;
        this.upcasterChain = builder.upcasterChain;
    }

    /**
     * Instantiate a Builder to be able to create a {@link DefaultKafkaMessageConverter}.
     * <p>
     * The {@link SequencingPolicy} is defaulted to an {@link SequentialPerAggregatePolicy}, and the {@code
     * headerValueMapper} is defaulted to the {@link HeaderUtils#byteMapper()} function. The {@link Serializer} is a
     * <b>hard requirement</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link DefaultKafkaMessageConverter}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that the {@link ProducerRecord} created through this method sets the {@link ProducerRecord#timestamp()} to
     * {@code null}. Doing so will ensure the used Producer sets a timestamp itself for the record. The {@link
     * EventMessage#getTimestamp()} field is however still taken into account, but as headers.
     * <p>
     * Additional note that the ProducerRecord will be given a {@code null} {@link ProducerRecord#partition()} value. In
     * return, the {@link ProducerRecord#key()} field is defined by using the configured {@link SequencingPolicy} to
     * retrieve the given {@code eventMessage}'s {@code sequenceIdentifier}. The combination of a {@code null} partition
     * and the possibly present or empty key will define which partition the Producer will choose to dispatch the record
     * on.
     *
     * @see ProducerRecord
     */
    @Override
    public ProducerRecord<String, byte[]> createKafkaMessage(EventMessage<?> eventMessage, String topic) {
        SerializedObject<byte[]> serializedObject = eventMessage.serializePayload(serializer, byte[].class);
        return new ProducerRecord<>(
                topic, null, null, recordKey(eventMessage),
                serializedObject.getData(),
                toHeaders(eventMessage, serializedObject, headerValueMapper)
        );
    }

    private String recordKey(EventMessage<?> eventMessage) {
        Object sequenceIdentifier = sequencingPolicy.getSequenceIdentifierFor(eventMessage);
        return sequenceIdentifier != null ? sequenceIdentifier.toString() : null;
    }

    @Override
    public Optional<EventMessage<?>> readKafkaMessage(ConsumerRecord<String, byte[]> consumerRecord) {
        try {
            Headers headers = consumerRecord.headers();
            if (isAxonMessage(headers)) {
                byte[] messageBody = consumerRecord.value();
                final EventData<?> eventData = createEventData(headers, messageBody);
                return upcasterChain
                        .upcast(Stream.of(new InitialEventRepresentation(eventData, serializer)))
                        .findFirst()
                        .map(upcastedEventData -> new SerializedMessage<>(
                                     upcastedEventData.getMessageIdentifier(),
                                     new LazyDeserializingObject<>(upcastedEventData.getData(), serializer),
                                     upcastedEventData.getMetaData()
                             )
                        ).flatMap(serializedMessage -> buildMessage(headers, serializedMessage));
            }
        } catch (Exception e) {
            logger.trace("Error converting ConsumerRecord [{}] to an EventMessage", consumerRecord, e);
        }
        return Optional.empty();
    }

    /**
     * Constructs event data representation from given Kafka headers and byte array body.
     * <p>
     * This method <i>reuses</i> the {@link GenericDomainEventEntry} class for both types of events which can be
     * transmitted via Kafka. For domain events, the fields <code>aggregateType</code>, <code>aggregateId</code> and
     * <code>aggregateSeq</code> will contain the corresponding values, but for the simple event they will be
     * <code>null</code>. This is ok to pass <code>null</code> to those values and <code>0L</code> to
     * <code>aggregateSeq</code>, since the {@link InitialEventRepresentation} does the same in its constructor and
     * is implemented in a null-tolerant way. Check {@link DefaultKafkaMessageConverter#isDomainEvent(Headers)} for more
     * details.
     * </p>
     *
     * @param headers     Kafka headers.
     * @param messageBody Kafka payload as a byte array.
     * @return event data.
     */
    private EventData<?> createEventData(Headers headers, byte[] messageBody) {
        return new GenericDomainEventEntry<>(
                valueAsString(headers, AGGREGATE_TYPE),
                valueAsString(headers, AGGREGATE_ID),
                valueAsLong(headers, AGGREGATE_SEQ, 0L),
                valueAsString(headers, MESSAGE_ID),
                valueAsLong(headers, MESSAGE_TIMESTAMP),
                valueAsString(headers, MESSAGE_TYPE),
                valueAsString(headers, MESSAGE_REVISION, null),
                messageBody,
                extractMetadataAsBytes(headers)
        );
    }

    private byte[] extractMetadataAsBytes(Headers headers) {
        return serializer.serialize(MetaData.from(extractAxonMetadata(headers)), byte[].class).getData();
    }

    private static boolean isAxonMessage(Headers headers) {
        return keys(headers).containsAll(Arrays.asList(MESSAGE_ID, MESSAGE_TYPE));
    }

    /**
     * Checks if the event is originated from an aggregate (domain event) or is a simple event sent over the bus.
     * <p>
     * The difference between a DomainEventMessage and an EventMessage, is the following three fields:
     * <ul>
     *     <li>The type - represents the Aggregate the event originates from. It would be empty for an EventMessage and
     *     filled for a DomainEventMessage.</li>
     *     <li>The aggregateIdentifier - represents the Aggregate instance the event originates from. It would be equal
     *     to the eventIdentifier for an EventMessage and not equal to that identifier a DomainEventMessage.</li>
     *     <li>The sequenceNumber - represents the order of the events within an Aggregate instance's event stream.
     *     It would be 0 at all times for an EventMessage, whereas a DomainEventMessage would be 0 or greater.</li>
     * </ul>
     * </p>
     *
     * @param headers Kafka headers.
     * @return <code>true</code> if the event is originated from an aggregate.
     */
    private static boolean isDomainEvent(Headers headers) {
        return headers.lastHeader(AGGREGATE_TYPE) != null
                && headers.lastHeader(AGGREGATE_ID) != null
                && headers.lastHeader(AGGREGATE_SEQ) != null;
    }

    private static Optional<EventMessage<?>> buildMessage(Headers headers, SerializedMessage<?> message) {
        long timestamp = valueAsLong(headers, MESSAGE_TIMESTAMP);
        return isDomainEvent(headers)
                ? buildDomainEventMessage(headers, message, timestamp)
                : buildEventMessage(message, timestamp);
    }

    private static Optional<EventMessage<?>> buildDomainEventMessage(Headers headers, SerializedMessage<?> message,
                                                              long timestamp) {
        return Optional.of(new GenericDomainEventMessage<>(
                valueAsString(headers, AGGREGATE_TYPE),
                valueAsString(headers, AGGREGATE_ID),
                valueAsLong(headers, AGGREGATE_SEQ),
                message,
                () -> Instant.ofEpochMilli(timestamp)
        ));
    }

    private static Optional<EventMessage<?>> buildEventMessage(SerializedMessage<?> message, long timestamp) {
        return Optional.of(new GenericEventMessage<>(message, () -> Instant.ofEpochMilli(timestamp)));
    }

    /**
     * Builder class to instantiate a {@link DefaultKafkaMessageConverter}.
     * <p>
     * The {@link SequencingPolicy} is defaulted to an {@link SequentialPerAggregatePolicy}, and the {@code
     * headerValueMapper} is defaulted to the {@link HeaderUtils#byteMapper()} function. The {@link Serializer} is a
     * <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder {

        private Serializer serializer;
        private SequencingPolicy<? super EventMessage<?>> sequencingPolicy = SequentialPerAggregatePolicy.instance();
        private BiFunction<String, Object, RecordHeader> headerValueMapper = byteMapper();
        private EventUpcasterChain upcasterChain = new EventUpcasterChain();

        /**
         * Sets the serializer to serialize the Event Message's payload with.
         *
         * @param serializer The serializer to serialize the Event Message's payload with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Sets the {@link SequencingPolicy}, with a generic of being a super of {@link EventMessage}, used to generate
         * the key for the {@link ProducerRecord}. Defaults to a {@link SequentialPerAggregatePolicy} instance.
         *
         * @param sequencingPolicy a {@link SequencingPolicy} used to generate the key for the {@link ProducerRecord}
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder sequencingPolicy(SequencingPolicy<? super EventMessage<?>> sequencingPolicy) {
            assertNonNull(sequencingPolicy, "SequencingPolicy may not be null");
            this.sequencingPolicy = sequencingPolicy;
            return this;
        }

        /**
         * Sets the {@code headerValueMapper}, a {@link BiFunction} of {@link String}, {@link Object} and {@link
         * RecordHeader}, used for mapping values to Kafka headers. Defaults to the {@link HeaderUtils#byteMapper()}
         * function.
         *
         * @param headerValueMapper a {@link BiFunction} of {@link String}, {@link Object} and {@link RecordHeader},
         *                          used for mapping values to Kafka headers
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder headerValueMapper(BiFunction<String, Object, RecordHeader> headerValueMapper) {
            assertNonNull(headerValueMapper, "{} may not be null");
            this.headerValueMapper = headerValueMapper;
            return this;
        }

        /**
         * Sets the {@code upcasterChain} to be used during the consumption of events.
         *
         * @param upcasterChain upcaster chain to be used on event reading.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder upcasterChain(EventUpcasterChain upcasterChain) {
            assertNonNull(upcasterChain, "UpcasterChain must not be null");
            this.upcasterChain = upcasterChain;
            return this;
        }

        /**
         * Initializes a {@link DefaultKafkaMessageConverter} as specified through this Builder.
         *
         * @return a {@link DefaultKafkaMessageConverter} as specified through this Builder
         */
        public DefaultKafkaMessageConverter build() {
            return new DefaultKafkaMessageConverter(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
        }
    }
}
