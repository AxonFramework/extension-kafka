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

package org.axonframework.extensions.kafka.eventhandling.cloudevent;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventData;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.async.SequencingPolicy;
import org.axonframework.eventhandling.async.SequentialPerAggregatePolicy;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.InitialEventRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.extensions.kafka.eventhandling.cloudevent.ExtensionUtils.*;

/**
 * Converts and {@link EventMessage} to a {@link ProducerRecord} Kafka message and from a {@link ConsumerRecord} Kafka
 * message back to an EventMessage (if possible).
 * <p>
 * During conversion meta data entries with the {@code 'axon-metadata-'} prefix are passed to the {@link Headers}. Other
 * message-specific attributes are added as metadata. The {@link EventMessage#getPayload()} is serialized using the
 * configured {@link Serializer} and passed as the Kafka record's body.
 * <p>
 * <p>
 * If an up-caster / up-caster chain is configured, this converter will pass the converted messages through it. Please
 * note, that since the message converter consumes records one-by-one, the up-casting functionality is limited to
 * one-to-one and one-to-many up-casters only.
 * </p>
 * This implementation will suffice in most cases.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class CloudEventKafkaMessageConverter implements KafkaMessageConverter<String, CloudEvent> {

    private static final Logger logger = LoggerFactory.getLogger(CloudEventKafkaMessageConverter.class);

    private final Serializer serializer;
    private final SequencingPolicy<? super EventMessage<?>> sequencingPolicy;
    private final EventUpcasterChain upcasterChain;

    /**
     * Instantiate a {@link CloudEventKafkaMessageConverter} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Serializer} is not {@code null} and will throw an {@link AxonConfigurationException}
     * if it is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link CloudEventKafkaMessageConverter} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected CloudEventKafkaMessageConverter(Builder builder) {
        builder.validate();
        this.serializer = builder.serializer;
        this.sequencingPolicy = builder.sequencingPolicy;
        this.upcasterChain = builder.upcasterChain;
    }

    /**
     * Instantiate a Builder to be able to create a {@link CloudEventKafkaMessageConverter}.
     * <p>
     * The {@link SequencingPolicy} is defaulted to an {@link SequentialPerAggregatePolicy}. The {@link Serializer} is
     * a
     * <b>hard requirement</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link CloudEventKafkaMessageConverter}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that the {@link ProducerRecord} created through this method sets the {@link ProducerRecord#timestamp()} to
     * {@code null}. Doing so will ensure the used Producer sets a timestamp itself for the record. The
     * {@link EventMessage#getTimestamp()} field is however still taken into account, but as headers.
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
    public ProducerRecord<String, CloudEvent> createKafkaMessage(EventMessage<?> eventMessage, String topic) {
        SerializedObject<byte[]> serializedObject = eventMessage.serializePayload(serializer, byte[].class);
        return new ProducerRecord<>(
                topic, null, null, recordKey(eventMessage),
                toCloudEvent(eventMessage, serializedObject),
                null
        );
    }

    private CloudEvent toCloudEvent(EventMessage<?> message, SerializedObject<byte[]> serializedObject) {
        CloudEventBuilder builder = new CloudEventBuilder();
        builder.withId(message.getIdentifier());
        builder.withData(serializedObject.getData());
        builder.withSource(URI.create(message.getClass().getCanonicalName()));
        builder.withType(serializedObject.getType().getName());
        builder.withTime(message.getTimestamp().atOffset(ZoneOffset.UTC));
        builder.withExtension(MESSAGE_REVISION, serializedObject.getType().getRevision());
        if (message instanceof DomainEventMessage) {
            DomainEventMessage<?> domainMessage = (DomainEventMessage<?>) message;
            builder.withExtension(AGGREGATE_ID, domainMessage.getAggregateIdentifier());
            builder.withExtension(AGGREGATE_SEQ, domainMessage.getSequenceNumber());
            builder.withExtension(AGGREGATE_TYPE, domainMessage.getType());
        }
        message.getMetaData().entrySet().forEach(entry -> setExtension(builder, entry));
        return builder.build();
    }

    private String recordKey(EventMessage<?> eventMessage) {
        Object sequenceIdentifier = sequencingPolicy.getSequenceIdentifierFor(eventMessage);
        return sequenceIdentifier != null ? sequenceIdentifier.toString() : null;
    }

    @Override
    public Optional<EventMessage<?>> readKafkaMessage(ConsumerRecord<String, CloudEvent> consumerRecord) {
        try {
            CloudEvent cloudEvent = consumerRecord.value();
            final EventData<?> eventData = createEventData(cloudEvent);
            return upcasterChain
                    .upcast(Stream.of(new InitialEventRepresentation(eventData, serializer)))
                    .findFirst()
                    .map(upcastedEventData -> new SerializedMessage<>(
                                 upcastedEventData.getMessageIdentifier(),
                                 new LazyDeserializingObject<>(upcastedEventData.getData(), serializer),
                                 upcastedEventData.getMetaData()
                         )
                    ).flatMap(serializedMessage -> buildMessage(cloudEvent, serializedMessage));
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
     * is implemented in a null-tolerant way. Check {@link CloudEventKafkaMessageConverter#isDomainEvent(CloudEvent)}
     * for more details.
     * </p>
     *
     * @param cloudEvent the event read from Kafka, serialized by the {@link io.cloudevents.kafka.CloudEventSerializer}
     * @return event data.
     */
    private EventData<?> createEventData(CloudEvent cloudEvent) {
        return new GenericDomainEventEntry<>(
                asNullableString(cloudEvent.getExtension(AGGREGATE_TYPE)),
                asNullableString(cloudEvent.getExtension(AGGREGATE_ID)),
                asLong(cloudEvent.getExtension(AGGREGATE_SEQ)),
                cloudEvent.getId(),
                asOffsetDateTime(cloudEvent.getTime()),
                cloudEvent.getType(),
                asNullableString(cloudEvent.getExtension(MESSAGE_REVISION)),
                asBytes(cloudEvent.getData()),
                extractMetadataAsBytes(cloudEvent)
        );
    }

    private byte[] extractMetadataAsBytes(CloudEvent cloudEvent) {
        Map<String, Object> metadataMap = new HashMap<>();
        cloudEvent.getExtensionNames().forEach(name -> {
            if (!isNonMetadataExtension(name)) {
                metadataMap.put(name, cloudEvent.getExtension(name));
            }
        });
        return serializer.serialize(MetaData.from(metadataMap), byte[].class).getData();
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
     * @param cloudEvent Cloud Event
     * @return <code>true</code> if the event is originated from an aggregate.
     */
    private static boolean isDomainEvent(CloudEvent cloudEvent) {
        return cloudEvent.getExtension(AGGREGATE_TYPE) != null
                && cloudEvent.getExtension(AGGREGATE_ID) != null
                && cloudEvent.getExtension(AGGREGATE_SEQ) != null;
    }

    private static Optional<EventMessage<?>> buildMessage(CloudEvent cloudEvent, SerializedMessage<?> message) {
        Instant timestamp = Instant.from(asOffsetDateTime(cloudEvent.getTime()));
        return isDomainEvent(cloudEvent)
                ? buildDomainEventMessage(cloudEvent, message, timestamp)
                : buildEventMessage(message, timestamp);
    }

    private static Optional<EventMessage<?>> buildDomainEventMessage(CloudEvent cloudEvent,
                                                                     SerializedMessage<?> message,
                                                                     Instant timestamp) {
        return Optional.of(new GenericDomainEventMessage<>(
                asNullableString(cloudEvent.getExtension(AGGREGATE_TYPE)),
                asNullableString(cloudEvent.getExtension(AGGREGATE_ID)),
                asLong(cloudEvent.getExtension(AGGREGATE_SEQ)),
                message,
                () -> timestamp
        ));
    }

    private static Optional<EventMessage<?>> buildEventMessage(SerializedMessage<?> message, Instant timestamp) {
        return Optional.of(new GenericEventMessage<>(message, () -> timestamp));
    }

    /**
     * Builder class to instantiate a {@link CloudEventKafkaMessageConverter}.
     * <p>
     * The {@link SequencingPolicy} is defaulted to an {@link SequentialPerAggregatePolicy}. The {@link Serializer} is
     * a
     * <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder {

        private Serializer serializer;
        private SequencingPolicy<? super EventMessage<?>> sequencingPolicy = SequentialPerAggregatePolicy.instance();
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
         * Initializes a {@link CloudEventKafkaMessageConverter} as specified through this Builder.
         *
         * @return a {@link CloudEventKafkaMessageConverter} as specified through this Builder
         */
        public CloudEventKafkaMessageConverter build() {
            return new CloudEventKafkaMessageConverter(this);
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
