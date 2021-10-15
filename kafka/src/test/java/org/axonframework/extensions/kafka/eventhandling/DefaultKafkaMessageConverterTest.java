/*
 * Copyright (c) 2010-2021. Axon Framework
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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.AnyTypePermission;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.FixedValueRevisionResolver;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;
import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;
import static org.axonframework.extensions.kafka.eventhandling.util.HeaderAssertUtil.assertDomainHeaders;
import static org.axonframework.extensions.kafka.eventhandling.util.HeaderAssertUtil.assertEventHeaders;
import static org.axonframework.messaging.Headers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultKafkaMessageConverter}.
 *
 * @author Lucas Campos
 * @author Steven van Beelen
 * @author Nakul Mishra
 */
class DefaultKafkaMessageConverterTest {

    private static final String SOME_TOPIC = "topicFoo";
    private static final int SOME_OFFSET = 0;
    private static final int SOME_PARTITION = 0;
    private static final String SOME_AGGREGATE_IDENTIFIER = "1234";

    private DefaultKafkaMessageConverter testSubject;
    private XStreamSerializer serializer;

    private static void assertEventMessage(EventMessage<?> actual, EventMessage<?> expected) {
        assertEquals(expected.getIdentifier(), actual.getIdentifier());
        assertEquals(expected.getPayloadType(), actual.getPayloadType());
        assertEquals(expected.getMetaData(), actual.getMetaData());
        assertEquals(expected.getPayload(), actual.getPayload());
        assertEquals(expected.getTimestamp().toEpochMilli(), actual.getTimestamp().toEpochMilli());
    }

    private static EventMessage<Object> eventMessage() {
        return asEventMessage("SomePayload").withMetaData(MetaData.with("key", "value"));
    }

    private static GenericDomainEventMessage<String> domainMessage() {
        return new GenericDomainEventMessage<>(
            "Stub", SOME_AGGREGATE_IDENTIFIER, 1L, "Payload", MetaData.with("key", "value")
        );
    }

    private static ConsumerRecord<String, byte[]> toReceiverRecord(ProducerRecord<String, byte[]> message) {
        ConsumerRecord<String, byte[]> receiverRecord = new ConsumerRecord<>(
            SOME_TOPIC, SOME_PARTITION, SOME_OFFSET, message.key(), message.value()
        );
        message.headers().forEach(header -> receiverRecord.headers().add(header));
        return receiverRecord;
    }

    @BeforeEach
    void setUp() {
        final XStream xStream = new XStream();
        xStream.addPermission(AnyTypePermission.ANY);
        serializer = XStreamSerializer.builder()
                                      .revisionResolver(new FixedValueRevisionResolver("stub-revision"))
                                      .xStream(xStream)
                                      .build();
        testSubject = DefaultKafkaMessageConverter.builder().serializer(serializer).build();
    }

    @Test
    void testKafkaKeyGenerationEventMessageShouldBeNull() {
        ProducerRecord<String, byte[]> evt = testSubject.createKafkaMessage(eventMessage(), SOME_TOPIC);

        assertNull(evt.key());
    }

    @Test
    void testKafkaKeyGenerationDomainMessageShouldBeAggregateIdentifier() {
        ProducerRecord<String, byte[]> domainEvt = testSubject.createKafkaMessage(domainMessage(), SOME_TOPIC);

        assertEquals(domainMessage().getAggregateIdentifier(), domainEvt.key());
    }

    @Test
    void testWritingEventMessageAsKafkaMessageShouldAppendEventHeaders() {
        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        SerializedObject<byte[]> serializedObject = expected.serializePayload(serializer, byte[].class);

        assertEventHeaders("key", expected, serializedObject, senderMessage.headers());
    }

    @Test
    void testWritingDomainMessageAsKafkaMessageShouldAppendDomainHeaders() {
        GenericDomainEventMessage<String> expected = domainMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);

        assertDomainHeaders(expected, senderMessage.headers());
    }

    @Test
    void testReadingMessage_WhenKafkaReturnNullHeadersShouldReturnEmptyMessage() {
        //noinspection unchecked
        ConsumerRecord<String, byte[]> source = mock(ConsumerRecord.class);
        when(source.headers()).thenReturn(null);

        assertFalse(testSubject.readKafkaMessage(source).isPresent());
    }

    @Test
    void testReadingMessageMissingAxonHeaderShouldReturnEmptyMessage() {
        ConsumerRecord<String, byte[]> msgWithoutHeaders = new ConsumerRecord<>("foo", 0, 0, "abc", new byte[0]);

        assertFalse(testSubject.readKafkaMessage(msgWithoutHeaders).isPresent());
    }

    @Test
    void testReadingMessageWithoutIdShouldReturnEmptyMessage() {
        EventMessage<?> event = eventMessage();
        ProducerRecord<String, byte[]> msg = testSubject.createKafkaMessage(event, SOME_TOPIC);
        msg.headers().remove(MESSAGE_ID);

        assertFalse(testSubject.readKafkaMessage(toReceiverRecord(msg)).isPresent());
    }

    @Test
    void testReadingMessageWithoutTypeShouldReturnEmptyMessage() {
        EventMessage<?> event = eventMessage();
        ProducerRecord<String, byte[]> msg = testSubject.createKafkaMessage(event, SOME_TOPIC);
        msg.headers().remove(MESSAGE_TYPE);

        assertFalse(testSubject.readKafkaMessage(toReceiverRecord(msg)).isPresent());
    }

    @Test
    void testReadingMessagePayloadDifferentThanByteShouldReturnEmptyMessage() {
        EventMessage<Object> eventMessage = eventMessage();
        //noinspection unchecked
        SerializedObject<byte[]> serializedObject = mock(SerializedObject.class);
        when(serializedObject.getType()).thenReturn(new SimpleSerializedType("foo", null));
        Headers headers = toHeaders(eventMessage, serializedObject, byteMapper());

        //noinspection rawtypes
        ConsumerRecord payloadDifferentThanByte = new ConsumerRecord<>(
            "foo", 0, 0, NO_TIMESTAMP, NO_TIMESTAMP_TYPE,
            -1L, NULL_SIZE, NULL_SIZE, "123", "some-wrong-input", headers
        );

        //noinspection unchecked
        assertFalse(testSubject.readKafkaMessage(payloadDifferentThanByte).isPresent());
    }

    @Test
    void testWritingEventMessageShouldBeReadAsEventMessageAndPassUpcaster() {
        AtomicInteger upcasterCalled = new AtomicInteger(0);

        EventUpcasterChain chain = new EventUpcasterChain(intermediateRepresentations -> {
            upcasterCalled.addAndGet(1);
            return intermediateRepresentations;
        });

        testSubject = DefaultKafkaMessageConverter.builder().serializer(serializer).upcasterChain(chain).build();

        EventMessage<?> expected = eventMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
        EventMessage<?> actual = receiverMessage(senderMessage);

        assertEventMessage(actual, expected);
        assertEquals(1, upcasterCalled.get());
    }

    @Test
    void testWritingEventMessageWithNullRevisionShouldWriteRevisionAsNull() {
        testSubject = DefaultKafkaMessageConverter.builder()
                                                  .serializer(XStreamSerializer.builder().build())
                                                  .build();
        EventMessage<?> eventMessage = eventMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(eventMessage, SOME_TOPIC);

        assertNull(valueAsString(senderMessage.headers(), MESSAGE_REVISION));
    }

    @Test
    void testWritingDomainEventMessageShouldBeReadAsDomainMessage() {
        DomainEventMessage<?> expected = domainMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
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
        testSubject = DefaultKafkaMessageConverter.builder().serializer(serializer).upcasterChain(chain).build();

        DomainEventMessage<?> expected = domainMessage();
        ProducerRecord<String, byte[]> senderMessage = testSubject.createKafkaMessage(expected, SOME_TOPIC);
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


    private void assertDomainMessage(DomainEventMessage<?> actual, DomainEventMessage<?> expected) {
        assertEquals(expected.getAggregateIdentifier(), actual.getAggregateIdentifier());
        assertEquals(expected.getSequenceNumber(), actual.getSequenceNumber());
        assertEquals(expected.getType(), actual.getType());
    }

    private EventMessage<?> receiverMessage(ProducerRecord<String, byte[]> senderMessage) {
        return testSubject.readKafkaMessage(
            toReceiverRecord(senderMessage)).orElseThrow(() -> new AssertionError("Expected valid message")
        );
    }
}
