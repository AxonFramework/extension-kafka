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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.junit.jupiter.api.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;
import static org.axonframework.extensions.kafka.eventhandling.util.HeaderAssertUtil.assertDomainHeaders;
import static org.axonframework.extensions.kafka.eventhandling.util.HeaderAssertUtil.assertEventHeaders;
import static org.axonframework.messaging.Headers.MESSAGE_METADATA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link HeaderUtils}.
 *
 * @author Nakul Mishra
 */
class HeaderUtilsTest {

    private static double doubleValue(RecordHeaders target) {
        return ByteBuffer.wrap(Objects.requireNonNull(value(target, "double"))).getDouble();
    }

    private static float floatValue(RecordHeaders target) {
        return ByteBuffer.wrap(Objects.requireNonNull(value(target, "float"))).getFloat();
    }

    private static long longValue(RecordHeaders target) {
        return ByteBuffer.wrap(Objects.requireNonNull(value(target, "long"))).getLong();
    }

    private static int intValue(RecordHeaders target) {
        return ByteBuffer.wrap(Objects.requireNonNull(value(target, "int"))).getInt();
    }

    private static short shortValue(RecordHeaders target) {
        return ByteBuffer.wrap(Objects.requireNonNull(value(target, "short"))).getShort();
    }

    @SuppressWarnings("unchecked")
    private static SerializedObject<byte[]> serializedObject() {
        SerializedObject serializedObject = mock(SerializedObject.class);
        when(serializedObject.getType()).thenReturn(new SimpleSerializedType("someObjectType", "10"));
        return serializedObject;
    }

    @Test
    void testReadingValueAsBytesExistingKeyShouldReturnBytes() {
        RecordHeaders headers = new RecordHeaders();
        String value = "a1b2";
        addHeader(headers, "bar", value);

        assertEquals(value.getBytes(), value(headers, "bar"));
    }

    @Test
    void testReadingValuesAsBytesNonExistingKeyShouldReturnNull() {
        assertNull(value(new RecordHeaders(), "123"));
    }

    @Test
    void testReadingValueFromNullHeaderShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> value(null, "bar"));
    }

    @Test
    void testReadingValuesAsTextExistingKeyShouldReturnText() {
        RecordHeaders headers = new RecordHeaders();
        String expectedValue = "Şơм℮ śẩмρŀę ÅŚÇÍỈ-ťęҳť FFlETYeKU3H5QRqw";
        addHeader(headers, "foo", expectedValue);

        assertEquals(expectedValue, valueAsString(headers, "foo"));
        assertEquals(expectedValue, valueAsString(headers, "foo", "default-value"));
    }

    @Test
    void testReadingValueAsTextNonExistingKeyShouldReturnNull() {
        assertNull(valueAsString(new RecordHeaders(), "some-invalid-key"));
    }

    @Test
    void testReadingValueAsTextNonExistingKeyShouldReturnDefaultValue() {
        assertEquals("default-value", valueAsString(new RecordHeaders(), "some-invalid-key", "default-value"));
    }

    @Test
    void testReadingValuesAsLongExistingKeyShouldReturnLong() {
        RecordHeaders headers = new RecordHeaders();
        addHeader(headers, "positive", 4_891_00_921_388_62621L);
        addHeader(headers, "zero", 0L);
        addHeader(headers, "negative", -4_8912_00_921_388_62621L);

        assertEquals(4_891_00_921_388_62621L, valueAsLong(headers, "positive"));
        assertEquals(0, valueAsLong(headers, "zero"));
        assertEquals(-4_8912_00_921_388_62621L, valueAsLong(headers, "negative"));
    }

    @Test
    void testReadingValueAsLongNonExistingKeyShouldReturnNull() {
        assertNull(valueAsLong(new RecordHeaders(), "some-invalid-key"));
    }

    @Test
    void testWritingTimestampShouldBeWrittenAsLong() {
        RecordHeaders target = new RecordHeaders();
        Instant value = Instant.now();
        addHeader(target, "baz", value);

        assertEquals(value.toEpochMilli(), valueAsLong(target, "baz"));
    }

    @Test
    void testWritingNonNegativeValuesShouldBeWrittenAsNonNegativeValues() {
        RecordHeaders target = new RecordHeaders();
        short expectedShort = 1;
        int expectedInt = 200;
        long expectedLong = 300L;
        float expectedFloat = 300.f;
        double expectedDouble = 0.000;

        addHeader(target, "short", expectedShort);
        assertEquals(expectedShort, shortValue(target));

        addHeader(target, "int", expectedInt);
        assertEquals(expectedInt, intValue(target));

        addHeader(target, "long", expectedLong);
        assertEquals(expectedLong, longValue(target));

        addHeader(target, "float", expectedFloat);
        assertEquals(expectedFloat, floatValue(target));

        addHeader(target, "double", expectedDouble);
        assertEquals(expectedDouble, doubleValue(target));
    }

    @Test
    void testWritingNegativeValuesShouldBeWrittenAsNegativeValues() {
        RecordHeaders target = new RecordHeaders();
        short expectedShort = -123;
        int expectedInt = -1_234_567_8;
        long expectedLong = -1_234_567_89_0L;
        float expectedFloat = -1_234_567_89_0.0f;
        double expectedDouble = -1_234_567_89_0.987654321;

        addHeader(target, "short", expectedShort);
        assertEquals(expectedShort, shortValue(target));

        addHeader(target, "int", expectedInt);
        assertEquals(expectedInt, intValue(target));

        addHeader(target, "long", expectedLong);
        assertEquals(expectedLong, longValue(target));

        addHeader(target, "float", expectedFloat);
        assertEquals(expectedFloat, floatValue(target));

        addHeader(target, "double", expectedDouble);
        assertEquals(expectedDouble, doubleValue(target));
    }

    @Test
    void testWritingNonPrimitiveJavaValueShouldThrowAnException() {
        assertThrows(IllegalArgumentException.class, () -> addHeader(new RecordHeaders(), "short", BigInteger.ZERO));
    }

    @Test
    void testWritingTextValueShouldBeWrittenAsString() {
        RecordHeaders target = new RecordHeaders();
        String expectedKey = "foo";
        String expectedValue = "a";
        addHeader(target, expectedKey, expectedValue);

        assertEquals(1, target.toArray().length);
        assertEquals(expectedKey, target.lastHeader(expectedKey).key());
        assertEquals(expectedValue, valueAsString(target, expectedKey));
    }

    @Test
    void testWritingNullValueShouldBeWrittenAsNull() {
        RecordHeaders target = new RecordHeaders();
        addHeader(target, "baz", null);

        assertNull(value(target, "baz"));
    }

    @Test
    void testWritingCustomValueShouldBeWrittenAsRepresentedByToString() {
        RecordHeaders target = new RecordHeaders();
        Foo expectedValue = new Foo("someName", new Bar(100));
        addHeader(target, "object", expectedValue);

        assertEquals(expectedValue.toString(), valueAsString(target, "object"));
    }

    @Test
    void testExtractingExistingKeysShouldReturnAllKeys() {
        RecordHeaders target = new RecordHeaders();
        addHeader(target, "a", "someValue");
        addHeader(target, "b", "someValue");
        addHeader(target, "c", "someValue");
        Set<String> expectedKeys = new HashSet<>();
        target.forEach(header -> expectedKeys.add(header.key()));

        assertEquals(expectedKeys, keys(target));
    }

    @Test
    void testExtractingKeysFromNullHeaderShouldThrowAnException() {
        assertThrows(IllegalArgumentException.class, () -> keys(null));
    }

    @Test
    void testGeneratingKeyForSendingAxonMetadataToKafkaShouldGenerateCorrectKeys() {
        assertEquals(MESSAGE_METADATA + "-foo", generateMetadataKey("foo"));
        assertEquals(MESSAGE_METADATA + "-null", generateMetadataKey(null));
    }

    @Test
    void testExtractingKeyForSendingAxonMetadataToKafkaShouldReturnActualKey() {
        assertEquals("foo", extractKey(generateMetadataKey("foo")));
    }

    @Test
    void testExtractingKeyNullMetadataKeyShouldThrowAnException() {
        assertThrows(IllegalArgumentException.class, () -> extractKey(null));
    }

    @Test
    void testExtractingKeyNonExistingMetadataKeyShouldThrowAnException() {
        assertThrows(IllegalArgumentException.class, () -> extractKey("foo-bar-axon-metadata"));
    }

    @Test
    void testExtractingAxonMetadataShouldReturnMetadata() {
        RecordHeaders target = new RecordHeaders();
        String key = generateMetadataKey("headerKey");
        String value = "abc";
        Map<String, Object> expectedValue = new HashMap<>();
        expectedValue.put("headerKey", value);
        addHeader(target, key, value);

        assertEquals(expectedValue, extractAxonMetadata(target));
    }

    @Test
    void testExtractingAxonMetadataFromNullHeaderShouldThrowAnException() {
        assertThrows(IllegalArgumentException.class, () -> extractAxonMetadata(null));
    }

    @Test
    void testGeneratingHeadersForEventMessageShouldGenerateEventHeaders() {
        String metaKey = "someHeaderKey";
        EventMessage<Object> evt = asEventMessage("SomePayload").withMetaData(
                MetaData.with(metaKey, "someValue")
        );
        SerializedObject<byte[]> so = serializedObject();
        Headers headers = toHeaders(evt, so, byteMapper());

        assertEventHeaders(metaKey, evt, so, headers);
    }

    @Test
    void testGeneratingHeadersForDomainMessageShouldGenerateBothEventAndDomainHeaders() {
        String metaKey = "someHeaderKey";
        DomainEventMessage<Object> evt =
                new GenericDomainEventMessage<>("Stub", "axc123-v", 1L, "Payload", MetaData.with("key", "value"));
        SerializedObject<byte[]> so = serializedObject();
        Headers headers = toHeaders(evt, so, byteMapper());

        assertEventHeaders(metaKey, evt, so, headers);
        assertDomainHeaders(evt, headers);
    }

    @Test
    void testByteMapperNullValueShouldBeAbleToHandle() {
        BiFunction<String, Object, RecordHeader> fxn = byteMapper();
        RecordHeader header = fxn.apply("abc", null);

        assertNull(header.value());
    }

    @Test
    void testGeneratingHeadersWithByteMapperShouldGenerateCorrectHeaders() {
        BiFunction<String, Object, RecordHeader> fxn = byteMapper();
        String expectedKey = "abc";
        String expectedValue = "xyz";
        RecordHeader header = fxn.apply(expectedKey, expectedValue);

        assertEquals(expectedKey, header.key());
        assertEquals(expectedValue, new String(header.value()));
    }

    @Test
    void testGeneratingHeadersWithCustomMapperShouldGeneratedCorrectHeaders() {
        String metaKey = "someHeaderKey";
        String expectedMetaDataValue = "evt:someValue";
        Headers header = toHeaders(
                asEventMessage("SomePayload").withMetaData(MetaData.with(metaKey, "someValue")),
                serializedObject(),
                (key, value) -> new RecordHeader(key, ("evt:" + value.toString()).getBytes())
        );

        assertEquals(expectedMetaDataValue, valueAsString(header, generateMetadataKey(metaKey)));
    }

    private static class Foo {

        private final String name;
        private final Bar bar;

        Foo(String name, Bar bar) {
            this.name = name;
            this.bar = bar;
        }

        @Override
        public String toString() {
            return "Foo{" +
                    "name='" + name + '\'' +
                    ", bar=" + bar +
                    '}';
        }
    }

    private static class Bar {

        private int count;

        Bar(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "Bar{" +
                    "count=" + count +
                    '}';
        }
    }
}