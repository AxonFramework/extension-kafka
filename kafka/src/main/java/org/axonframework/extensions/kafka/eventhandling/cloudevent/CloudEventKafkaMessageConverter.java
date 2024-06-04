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
import org.axonframework.common.AxonConfigurationException;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.cloudevent.ExtensionUtils.*;
import static org.axonframework.extensions.kafka.eventhandling.cloudevent.MetadataUtils.*;

/**
 * Converts an {@link EventMessage} to a {@link ProducerRecord} Kafka message and from a {@link ConsumerRecord} Kafka
 * message back to an EventMessage (if possible).
 * <p>
 * During conversion, metadata entries are stored as extensions. This might require adding mappings with either
 * {@link Builder#addMetadataMapper(String, String)} or {@link Builder#addMetadataMappers(Map)}. For the {@code source}
 * field, we default to <a href="https://www.axoniq.io/">https://www.axoniq.io/</a>. You can set the
 * {@link Builder#sourceSupplier(Function) sourceSupplier} to change this. For the {@code subject} field, we default to
 * setting it from the metadata. You can set the {@link Builder#subjectSupplier(Function) subjectSupplier} to change
 * this. For the {@code dataContentType} field, we default to setting it from the metadata. You can set the
 * {@link Builder#dataContentTypeSupplier(Function) dataContentTypeSupplier} to change this. For the {@code dataSchema}
 * field, we default to setting it from the metadata. You can set the
 * {@link Builder#dataSchemaSupplier(Function)  dataSchemaSupplier} to change this. This converter maps other
 * message-specific attributes to those best matching Cloud Events. The {@link EventMessage#getPayload()} is serialized
 * using the configured {@link Serializer} and passed as byte-array to the Cloud Event data field.
 * <p>
 * <p>
 * If you have configured an up-caster / up-caster chain, this converter will pass the converted messages through the
 * chain. Please note that since the message converter consumes records one-by-one, the up-casting functionality is
 * limited to one-to-one and one-to-many up-casters only.
 * </p>
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class CloudEventKafkaMessageConverter implements KafkaMessageConverter<String, CloudEvent> {

    private static final Logger logger = LoggerFactory.getLogger(CloudEventKafkaMessageConverter.class);

    private final Serializer serializer;
    private final SequencingPolicy<? super EventMessage<?>> sequencingPolicy;
    private final EventUpcasterChain upcasterChain;
    private final Map<String, String> extensionNameResolver;
    private final Map<String, String> metadataNameResolver;
    private final Function<EventMessage<?>, URI> sourceSupplier;
    private final Function<EventMessage<?>, Optional<String>> subjectSupplier;
    private final Function<EventMessage<?>, Optional<String>> dataContentTypeSupplier;
    private final Function<EventMessage<?>, Optional<URI>> dataSchemaSupplier;
    private final boolean ignoreInvalidExtensionNames;

    /**
     * Instantiate a {@link CloudEventKafkaMessageConverter} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Serializer} is not {@code null} and will throw an {@link AxonConfigurationException}
     * if it is {@code null}.
     * <p>
     * The {@code upcasterChain} is defaulted to a new {@link EventUpcasterChain}. During conversion, metadata entries
     * are stored as extensions. This might require adding mappings with either
     * {@link Builder#addMetadataMapper(String, String)} or {@link Builder#addMetadataMappers(Map)}. For the
     * {@code source} field, we default to <a href="https://www.axoniq.io/">https://www.axoniq.io/</a>. You can set the
     * {@link Builder#sourceSupplier(Function) sourceSupplier} to change this. For the {@code subject} field, we default
     * to setting it from the metadata. You can set the {@link Builder#subjectSupplier(Function) subjectSupplier} to
     * change this. For the {@code dataContentType} field, we default to setting it from the metadata. You can set the
     * {@link Builder#dataContentTypeSupplier(Function) dataContentTypeSupplier} to change this. For the
     * {@code dataSchema} field, we default to setting it from the metadata. You can set the
     * {@link Builder#dataSchemaSupplier(Function)  dataSchemaSupplier} to change this.
     *
     * @param builder the {@link Builder} used to instantiate a {@link CloudEventKafkaMessageConverter} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected CloudEventKafkaMessageConverter(Builder builder) {
        builder.validate();
        this.serializer = builder.serializer;
        this.sequencingPolicy = builder.sequencingPolicy;
        this.upcasterChain = builder.upcasterChain;
        this.extensionNameResolver = builder.metadataToExtensionMap;
        this.metadataNameResolver = builder.metadataToExtensionMap.entrySet()
                                                                  .stream()
                                                                  .collect(Collectors.toMap(Map.Entry::getValue,
                                                                                            Map.Entry::getKey));
        this.sourceSupplier = builder.sourceSupplier;
        this.subjectSupplier = builder.subjectSupplier;
        this.dataContentTypeSupplier = builder.dataContentTypeSupplier;
        this.dataSchemaSupplier = builder.dataSchemaSupplier;
        this.ignoreInvalidExtensionNames = builder.ignoreInvalidExtensionNames;
    }

    /**
     * Instantiate a Builder to be able to create a {@link CloudEventKafkaMessageConverter}.
     * <p>
     * The {@link SequencingPolicy} is defaulted to an {@link SequentialPerAggregatePolicy}. The {@link Serializer} is a
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
     * {@link EventMessage#getTimestamp()} field is however still taken into account, but as the Cloud Event
     * {@code time} field.
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
        subjectSupplier.apply(message).ifPresent(builder::withSubject);
        dataContentTypeSupplier.apply(message).ifPresent(builder::withDataContentType);
        dataSchemaSupplier.apply(message).ifPresent(builder::withDataSchema);
        builder.withSource(sourceSupplier.apply(message));
        builder.withType(serializedObject.getType().getName());
        builder.withTime(message.getTimestamp().atOffset(ZoneOffset.UTC));
        setExtensions(builder, message, serializedObject, extensionNameResolver, ignoreInvalidExtensionNames);
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
            final EventData<?> eventData = createEventData(cloudEvent, consumerRecord.timestamp());
            return upcasterChain
                    .upcast(Stream.of(new InitialEventRepresentation(eventData, serializer)))
                    .findFirst()
                    .map(upcastedEventData -> new SerializedMessage<>(
                                 upcastedEventData.getMessageIdentifier(),
                                 new LazyDeserializingObject<>(upcastedEventData.getData(), serializer),
                                 upcastedEventData.getMetaData()
                         )
                    ).flatMap(serializedMessage -> buildMessage(cloudEvent,
                                                                serializedMessage,
                                                                consumerRecord.timestamp()));
        } catch (Exception e) {
            logger.trace("Error converting ConsumerRecord [{}] to an EventMessage", consumerRecord, e);
        }
        return Optional.empty();
    }

    /**
     * Constructs event data representation from given Cloud Event. Note that we rely on the
     * {@link io.cloudevents.kafka.CloudEventSerializer} to transform the Kafka value bytes, and optionally headers to a
     * {@link CloudEvent}.
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
    private EventData<?> createEventData(CloudEvent cloudEvent, long fallBackTimestamp) {
        return new GenericDomainEventEntry<>(
                asNullableString(cloudEvent.getExtension(AGGREGATE_TYPE)),
                asNullableString(cloudEvent.getExtension(AGGREGATE_ID)),
                asLong(cloudEvent.getExtension(AGGREGATE_SEQ)),
                cloudEvent.getId(),
                asOffsetDateTime(cloudEvent.getTime(), fallBackTimestamp),
                cloudEvent.getType(),
                asNullableString(cloudEvent.getExtension(MESSAGE_REVISION)),
                asBytes(cloudEvent.getData()),
                extractMetadataAsBytes(cloudEvent)
        );
    }

    private byte[] extractMetadataAsBytes(CloudEvent cloudEvent) {
        MetaData metaData = getExtensionsAsMetadata(cloudEvent, metadataNameResolver)
                .mergedWith(getAdditionalEntries(cloudEvent));
        return serializer.serialize(metaData, byte[].class).getData();
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
     * @param cloudEvent The {@link CloudEvent} to validate if it's a domain event.
     * @return {@code true} if the event is originated from an aggregate, {@code false} otherwise
     */
    private static boolean isDomainEvent(CloudEvent cloudEvent) {
        return cloudEvent.getExtension(AGGREGATE_TYPE) != null
                && cloudEvent.getExtension(AGGREGATE_ID) != null
                && cloudEvent.getExtension(AGGREGATE_SEQ) != null;
    }

    private static Optional<EventMessage<?>> buildMessage(CloudEvent cloudEvent, SerializedMessage<?> message,
                                                          long fallbackTimestamp) {
        Instant timestamp = Instant.from(asOffsetDateTime(cloudEvent.getTime(), fallbackTimestamp));
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
     * <b>hard requirement</b> and as such should be provided. The {@code upcasterChain} is defaulted to a new
     * {@link EventUpcasterChain}. The {@code metadataToExtensionMap} is defaulted to a {@link #tracingMap()}. The
     * {@code sourceSupplier} is defaulted to the class of the message. The {@code subjectSupplier} is defaulted to
     * getting the subject from the 'cloud-event-subject' metadata. The {@code dataContentTypeSupplier} is defaulted to
     * getting the subject from the 'cloud-event-data-content-type' metadata. The {@code dataSchemaSupplier} is
     * defaulted to getting the subject from the 'cloud-event-data-schema' metadata. The
     * {@code ignoreInvalidExtensionNames} is defaulted to not ignore extensions with invalid names thus leading
     * to throwing an {@link InvalidMetaDataException}
     */
    public static class Builder {

        private Serializer serializer;
        private SequencingPolicy<? super EventMessage<?>> sequencingPolicy = SequentialPerAggregatePolicy.instance();
        private EventUpcasterChain upcasterChain = new EventUpcasterChain();
        private final Map<String, String> metadataToExtensionMap = tracingMap();
        private Function<EventMessage<?>, URI> sourceSupplier = m -> URI.create("https://www.axoniq.io/");
        private Function<EventMessage<?>, Optional<String>> subjectSupplier = defaultSubjectSupplier();
        private Function<EventMessage<?>, Optional<String>> dataContentTypeSupplier = defaultDataContentTypeSupplier();
        private Function<EventMessage<?>, Optional<URI>> dataSchemaSupplier = defaultDataSchemaSupplier();
        private boolean ignoreInvalidExtensionNames = false;

        /**
         * Creates a new map, to convert the two metadata properties used for tracing, which are incompatible with cloud
         * event extension names.
         *
         * @return a map to convert the tracing metadata
         */
        private Map<String, String> tracingMap() {
            Map<String, String> map = new HashMap<>();
            map.put("traceId", "traceid");
            map.put("correlationId", "correlationid");
            return map;
        }

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
         * Sets the {@code upcasterChain} to be used during the consumption of events. By default, a new
         * {@link EventUpcasterChain} will be used.
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
         * Adds mappers to convert metadata keys to extension names. Please note that for extension names there is a
         * limitation as can be seen in {@link ExtensionUtils#isValidExtensionName(String)}. By default, just the two
         * properties used for tracing are included.
         *
         * @param metadataMappers The map to convert metadata keys to extension names, and the other way around
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder addMetadataMappers(Map<String, String> metadataMappers) {
            assertThat(metadataMappers, ExtensionUtils::isValidMetadataToExtensionMap,
                       "The metadataMappers has invalid extension names");
            this.metadataToExtensionMap.putAll(metadataMappers);
            return this;
        }

        /**
         * Adds one mapping to convert metadata keys to extension names. Please note that for extension names there is a
         * limitation as can be seen in {@link ExtensionUtils#isValidExtensionName(String)}. By default, just the two
         * properties used for tracing are included.
         *
         * @param metadataKey   the metadata key which is to be changed when used as extension
         * @param extensionName the extension name, this can only contain lowercase letters and digits
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder addMetadataMapper(String metadataKey, String extensionName) {
            assertThat(extensionName, ExtensionUtils::isValidExtensionName,
                       "The extension name is invalid");
            this.metadataToExtensionMap.put(metadataKey, extensionName);
            return this;
        }

        /**
         * Sets the {@code sourceSupplier} to be used to determine the source of the Cloud Event, this defaults to <a
         * href="https://www.axoniq.io/">https://www.axoniq.io/</a>.
         *
         * @param sourceSupplier the supplier of the source
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder sourceSupplier(Function<EventMessage<?>, URI> sourceSupplier) {
            assertNonNull(sourceSupplier, "sourceSupplier must not be null");
            this.sourceSupplier = sourceSupplier;
            return this;
        }

        /**
         * Sets the {@code dataContentTypeSupplier} to be used to determine the subject type of the Cloud Event, this
         * defaults to trying to get the value via the {@link MetadataUtils#SUBJECT} metadata.
         *
         * @param subjectSupplier the supplier of the subject
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder subjectSupplier(Function<EventMessage<?>, Optional<String>> subjectSupplier) {
            assertNonNull(subjectSupplier, "dataContentTypeSupplier must not be null");
            this.subjectSupplier = subjectSupplier;
            return this;
        }

        /**
         * Sets the {@code dataContentTypeSupplier} to be used to determine the data content type of the Cloud Event,
         * this defaults to trying to get the value via the {@link MetadataUtils#DATA_CONTENT_TYPE} metadata.
         *
         * @param dataContentTypeSupplier the supplier of the data content type
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder dataContentTypeSupplier(Function<EventMessage<?>, Optional<String>> dataContentTypeSupplier) {
            assertNonNull(dataContentTypeSupplier, "dataContentTypeSupplier must not be null");
            this.dataContentTypeSupplier = dataContentTypeSupplier;
            return this;
        }

        /**
         * Sets the {@code dataContentTypeSupplier} to be used to determine the data content type of the Cloud Event,
         * this defaults to trying to get the value via the {@link MetadataUtils#DATA_SCHEMA} metadata.
         *
         * @param dataSchemaSupplier the supplier of the data schema
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder dataSchemaSupplier(Function<EventMessage<?>, Optional<URI>> dataSchemaSupplier) {
            assertNonNull(dataSchemaSupplier, "dataSchemaSupplier must not be null");
            this.dataSchemaSupplier = dataSchemaSupplier;
            return this;
        }

        /**
         * if {@code true} than invalid extension names will not be added to {@link CloudEvent} message
         *
         * @param ignoreInvalidExtensionNames the ignore invalid extension names flag
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder ignoreInvalidExtensionNames(boolean ignoreInvalidExtensionNames) {
            this.ignoreInvalidExtensionNames = ignoreInvalidExtensionNames;
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
