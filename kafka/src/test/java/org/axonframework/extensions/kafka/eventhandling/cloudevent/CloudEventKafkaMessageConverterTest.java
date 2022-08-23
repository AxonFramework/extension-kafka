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

import com.thoughtworks.xstream.XStream;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.async.FullConcurrencyPolicy;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.FixedValueRevisionResolver;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.valueAsString;
import static org.axonframework.extensions.kafka.eventhandling.cloudevent.ExtensionUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link CloudEventKafkaMessageConverter}.
 *
 * @author Gerard Klijs
 */
class CloudEventKafkaMessageConverterTest {

    private static final String SOME_TOPIC = "topicFoo";
    private static final int SOME_OFFSET = 0;
    private static final int SOME_PARTITION = 0;
    private static final String SOME_AGGREGATE_IDENTIFIER = "1234";

    private CloudEventKafkaMessageConverter testSubject;
    private XStreamSerializer serializer;

    private static void assertEventMessage(EventMessage<?> actual, EventMessage<?> expected) {
        assertEquals(expected.getIdentifier(), actual.getIdentifier());
        assertEquals(expected.getPayloadType(), actual.getPayloadType());
        assertEquals(expected.getMetaData(), actual.getMetaData());
        assertEquals(expected.getPayload(), actual.getPayload());
        assertEquals(expected.getTimestamp().toEpochMilli(), actual.getTimestamp().toEpochMilli());
    }

    private static EventMessage<Object> eventMessage() {
        return asEventMessage("SomePayload")
                .withMetaData(
                        MetaData.with("key", "value")
                                .and("traceId", UUID.randomUUID().toString()));
    }

    private static GenericDomainEventMessage<String> domainMessage() {
        return new GenericDomainEventMessage<>(
                "Stub", SOME_AGGREGATE_IDENTIFIER, 1L, "Payload", MetaData.with("key", "value")
        );
    }

    private static ConsumerRecord<String, CloudEvent> toReceiverRecord(ProducerRecord<String, CloudEvent> message) {
        ConsumerRecord<String, CloudEvent> receiverRecord = new ConsumerRecord<>(
                SOME_TOPIC, SOME_PARTITION, SOME_OFFSET, message.key(), message.value()
        );
        message.headers().forEach(header -> receiverRecord.headers().add(header));
        return receiverRecord;
    }

    @BeforeEach
    void setUp() {
        XStream xStream = new XStream(new CompactDriver());
        xStream.allowTypesByWildcard(new String[]{"org.apache.kafka.**"});
        serializer = XStreamSerializer.builder()
                                      .xStream(xStream)
                                      .revisionResolver(new FixedValueRevisionResolver("stub-revision"))
                                      .build();
        testSubject = CloudEventKafkaMessageConverter.builder().serializer(serializer).build();
    }

    @Test
    void testKafkaKeyGenerationEventMessageShouldBeNull() {
        ProducerRecord<String, CloudEvent> evt = testSubject.createKafkaMessage(eventMessage(), SOME_TOPIC);

        assertNull(evt.key());
    }

    @Test
    void testKafkaKeyGenerationDomainMessageShouldBeAggregateIdentifier() {
        ProducerRecord<String, CloudEvent> domainEvt = testSubject.createKafkaMessage(domainMessage(), SOME_TOPIC);

        assertEquals(domainMessage().getAggregateIdentifier(), domainEvt.key());
    }

    @Test
    void testWritingEventMessageAsKafkaMessageShouldStoreMetaData() {
        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);

        Object storedValue = senderMessage.value().getExtension("key");
        assertEquals("value", storedValue);
    }

    @Test
    void testWritingDomainMessageAsKafkaMessageShouldAppendDomainHeaders() {
        GenericDomainEventMessage<String> expected = domainMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);

        CloudEvent event = senderMessage.value();
        assertNotNull(event.getExtension(AGGREGATE_TYPE));
        assertNotNull(event.getExtension(AGGREGATE_ID));
        assertNotNull(event.getExtension(AGGREGATE_SEQ));
    }

    @Test
    void testReadingMessage_WhenKafkaReturnNullHeadersShouldReturnEmptyMessage() {
        //noinspection unchecked
        ConsumerRecord<String, CloudEvent> source = mock(ConsumerRecord.class);
        when(source.headers()).thenReturn(null);

        assertFalse(testSubject.readKafkaMessage(source).isPresent());
    }

    @Test
    void testReadingMessageMissingAxonHeaderShouldReturnMessageAnyway() {
        ConsumerRecord<String, CloudEvent> msgWithoutHeaders =
                new ConsumerRecord<>("foo", 0, 0, "abc", minimalCloudEvent());
        assertTrue(testSubject.readKafkaMessage(msgWithoutHeaders).isPresent());
    }

    @Test
    void testWritingEventMessageShouldBeReadAsEventMessageAndPassUpcaster() {
        AtomicInteger upcasterCalled = new AtomicInteger(0);

        EventUpcasterChain chain = new EventUpcasterChain(intermediateRepresentations -> {
            upcasterCalled.addAndGet(1);
            return intermediateRepresentations;
        });

        testSubject = CloudEventKafkaMessageConverter.builder().serializer(serializer).upcasterChain(chain).build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertEquals(1, upcasterCalled.get());
    }

    @Test
    void testWritingEventMessageWithNullRevisionShouldWriteRevisionAsNull() {
        EventMessage<?> eventMessage = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(eventMessage, SOME_TOPIC);

        assertNull(valueAsString(senderMessage.headers(), MESSAGE_REVISION));
    }

    @Test
    void testWritingDomainEventMessageShouldBeReadAsDomainMessage() {
        DomainEventMessage<?> expected = domainMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertDomainMessage((DomainEventMessage<?>) actual, expected);
    }

    @Test
    void testWritingDomainEventMessageShouldBeReadAsDomainMessageAndPassUpcaster() {

        AtomicInteger upcasterCalled = new AtomicInteger(0);

        EventUpcasterChain chain = new EventUpcasterChain(intermediateRepresentations -> {
            upcasterCalled.addAndGet(1);
            return intermediateRepresentations;
        });
        testSubject = CloudEventKafkaMessageConverter.builder().serializer(serializer).upcasterChain(chain).build();

        DomainEventMessage<?> expected = domainMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertDomainMessage((DomainEventMessage<?>) actual, expected);
        assertEquals(1, upcasterCalled.get());
    }


    @Test
    void testBuildWithoutSerializerThrowsAxonConfigurationException() {
        DefaultKafkaMessageConverter.Builder testSubject = DefaultKafkaMessageConverter.builder();

        assertThrows(AxonConfigurationException.class, testSubject::build);
    }

    @Test
    void testBuildWithNullSerializerThrowsAxonConfigurationException() {
        DefaultKafkaMessageConverter.Builder testSubject = DefaultKafkaMessageConverter.builder();

        assertThrows(AxonConfigurationException.class, () -> testSubject.serializer(null));
    }

    @Test
    void testBuildWithNullSequencingPolicyThrowsAxonConfigurationException() {
        DefaultKafkaMessageConverter.Builder testSubject = DefaultKafkaMessageConverter.builder();

        assertThrows(AxonConfigurationException.class, () -> testSubject.sequencingPolicy(null));
    }

    @Test
    void testBuildWithNullHeaderValueMapperThrowsAxonConfigurationException() {
        DefaultKafkaMessageConverter.Builder testSubject = DefaultKafkaMessageConverter.builder();

        assertThrows(AxonConfigurationException.class, () -> testSubject.headerValueMapper(null));
    }

    @Test
    void testBuildWithNullUpcasterChainThrowsAxonConfigurationException() {
        DefaultKafkaMessageConverter.Builder testSubject = DefaultKafkaMessageConverter.builder();

        assertThrows(AxonConfigurationException.class, () -> testSubject.upcasterChain(null));
    }

    @Test
    void whenMetadataContainsReservedName_thenThrowAnError() {
        EventMessage<Object> eventMessage = asEventMessage("SomePayload").withMetaData(MetaData.with(AGGREGATE_TYPE,
                                                                                                     "value"));

        assertThrows(InvalidMetaDataException.class, () -> testSubject.createKafkaMessage(eventMessage, SOME_TOPIC));
    }

    @Test
    void whenMetadataContainsWrongName_thenThrowAnError() {
        EventMessage<Object> eventMessage = asEventMessage("SomePayload").withMetaData(MetaData.with("_KEY", "value"));

        assertThrows(InvalidMetaDataException.class, () -> testSubject.createKafkaMessage(eventMessage, SOME_TOPIC));
    }

    @Test
    void whenMetadataContainsUnsupportedValue_thenThrowAnError() {
        EventMessage<Object> eventMessage = asEventMessage("SomePayload")
                .withMetaData(MetaData.with("key", Collections.singletonList("value")));

        assertThrows(InvalidMetaDataException.class, () -> testSubject.createKafkaMessage(eventMessage, SOME_TOPIC));
    }

    @Test
    void testSequencingPolicyIsSet_thenItShouldBeUsed() {

        testSubject = CloudEventKafkaMessageConverter.builder()
                                                     .serializer(serializer)
                                                     .sequencingPolicy(new FullConcurrencyPolicy())
                                                     .build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        assertEquals(expected.getIdentifier(), senderMessage.key());
    }

    @Test
    void testWhenAddMetadataMappersIsSupplied_thenItShouldBeUsed() {
        Map<String, String> metadataToExtensionMap = new HashMap<>();
        metadataToExtensionMap.put("key", "foo");

        testSubject = CloudEventKafkaMessageConverter.builder()
                                                     .serializer(serializer)
                                                     .addMetadataMappers(metadataToExtensionMap)
                                                     .build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertEquals("value", senderMessage.value().getExtension("foo"));
    }

    @Test
    void testWhenAddMetadataMapperIsSupplied_thenItShouldBeUsed() {
        testSubject = CloudEventKafkaMessageConverter.builder()
                                                     .serializer(serializer)
                                                     .addMetadataMapper("key", "foo")
                                                     .build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertEquals("value", senderMessage.value().getExtension("foo"));
    }

    @Test
    void testSourceSupplierIsSet_thenItShouldBeUsed() {
        URI sourceUri = URI.create("https://github.com/AxonFramework/extension-kafka/");

        testSubject = CloudEventKafkaMessageConverter.builder()
                                                     .serializer(serializer)
                                                     .sourceSupplier(m -> sourceUri)
                                                     .build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, CloudEvent> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        assertEquals(sourceUri, senderMessage.value().getSource());
    }


    private void assertDomainMessage(DomainEventMessage<?> actual, DomainEventMessage<?> expected) {
        assertEquals(expected.getAggregateIdentifier(), actual.getAggregateIdentifier());
        assertEquals(expected.getSequenceNumber(), actual.getSequenceNumber());
        assertEquals(expected.getType(), actual.getType());
    }

    private EventMessage<?> receiverMessage(ProducerRecord<String, CloudEvent> senderMessage) {
        return testSubject.readKafkaMessage(
                toReceiverRecord(senderMessage)).orElseThrow(() -> new AssertionError("Expected valid message")
        );
    }

    private CloudEvent minimalCloudEvent() {
        return new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withTime(Instant.now().atOffset(ZoneOffset.UTC))
                .withSource(URI.create("org.axonframework.extensions.kafka.eventhandling.serialisation"))
                .withType(Object.class.getCanonicalName())
                .build();
    }
}
