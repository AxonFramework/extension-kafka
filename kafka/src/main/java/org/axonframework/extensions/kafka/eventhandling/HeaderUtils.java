/*
 * Copyright (c) 2010-2018. Axon Framework
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.SerializedObject;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.axonframework.common.Assert.isTrue;
import static org.axonframework.common.Assert.notNull;
import static org.axonframework.messaging.Headers.MESSAGE_METADATA;
import static org.axonframework.messaging.Headers.defaultHeaders;

/**
 * Utility class for dealing with {@link Headers}. Mostly for internal use.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public abstract class HeaderUtils {

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final String KEY_DELIMITER = "-";

    private HeaderUtils() {
        // Utility class
    }

    /**
     * Converts bytes to long.
     *
     * @param value the bytes to convert in to a long
     * @return the long build from the given bytes
     */
    public static Long asLong(byte[] value) {
        return value != null ? ByteBuffer.wrap(value).getLong() : null;
    }

    /**
     * Return a {@link Long} representation of the {@code value} stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code null} is returned.
     *
     * @param headers the Kafka {@code headers} to pull the {@link Long} value from
     * @param key     the key corresponding to the expected {@link Long} value
     * @return the value as a {@link Long} corresponding to the given {@code key} in the {@code headers}
     */
    public static Long valueAsLong(Headers headers, String key) {
        return asLong(value(headers, key));
    }

    /**
     * Converts bytes to {@link String}.
     *
     * @param value the bytes to convert in to a {@link String}
     * @return the {@link String} build from the given bytes
     */
    public static String asString(byte[] value) {
        return value != null ? new String(value, UTF_8) : null;
    }

    /**
     * Return a {@link String} representation of the {@code value} stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry {@code null} is returned.
     *
     * @param headers the Kafka {@code headers} to pull the {@link String} value from
     * @param key     the key corresponding to the expected {@link String} value
     * @return the value as a {@link String} corresponding to the given {@code key} in the {@code headers}
     */
    public static String valueAsString(Headers headers, String key) {
        return asString(value(headers, key));
    }

    /**
     * Return a {@link String} representation of the {@code value} stored under a given {@code key} inside the
     * {@link Headers}. In case of a missing entry the {@code defaultValue} is returned.
     *
     * @param headers      the Kafka {@code headers} to pull the {@link String} value from
     * @param key          the key corresponding to the expected {@link String} value
     * @param defaultValue the default value to return when {@code key} does not exist in the given {@code headers}
     * @return the value as a {@link String} corresponding to the given {@code key} in the {@code headers}
     */
    public static String valueAsString(Headers headers, String key, String defaultValue) {
        return Objects.toString(asString(value(headers, key)), defaultValue);
    }

    /**
     * Return the {@code value} stored under a given {@code key} inside the {@link Headers}. In case of missing entry
     * {@code null} is returned.
     *
     * @param headers the Kafka {@code headers} to pull the value from
     * @param key     the key corresponding to the expected value
     * @return the value corresponding to the given {@code key} in the {@code headers}
     */
    @SuppressWarnings("ConstantConditions") // Null check performed by `Assert.isTrue`
    public static byte[] value(Headers headers, String key) {
        isTrue(headers != null, () -> "Headers may not be null");
        Header header = headers.lastHeader(key);
        return header != null ? header.value() : null;
    }

    /**
     * Converts primitive arithmetic types to byte array.
     *
     * @param value the {@link Number} to convert into a byte array
     * @return the byte array converted from the given {@code value}
     */
    public static byte[] toBytes(Number value) {
        if (value instanceof Short) {
            return toBytes((Short) value);
        } else if (value instanceof Integer) {
            return toBytes((Integer) value);
        } else if (value instanceof Long) {
            return toBytes((Long) value);
        } else if (value instanceof Float) {
            return toBytes((Float) value);
        } else if (value instanceof Double) {
            return toBytes((Double) value);
        }
        throw new IllegalArgumentException("Cannot convert [" + value + "] to bytes");
    }

    private static byte[] toBytes(Short value) {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(value);
        return buffer.array();
    }

    private static byte[] toBytes(Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }

    private static byte[] toBytes(Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    private static byte[] toBytes(Float value) {
        ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
        buffer.putFloat(value);
        return buffer.array();
    }

    private static byte[] toBytes(Double value) {
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);
        return buffer.array();
    }

    /**
     * Creates a new {@link org.apache.kafka.common.header.internals.RecordHeader} based on {@code key} and
     * {@code value} and adds it to {@code headers}. The {@code value} is converted to bytes and follows this logic:
     * <ul>
     * <li>Instant - calls {@link Instant#toEpochMilli()}</li>
     * <li>Number - calls {@link HeaderUtils#toBytes} </li>
     * <li>String/custom object - calls {@link String#toString()} </li>
     * <li>null - <code>null</code> </li>
     * </ul>
     *
     * @param headers the Kafka {@code headers} to add a {@code key}/{@code value} pair to
     * @param key     the key you want to add to the {@code headers}
     * @param value   the value you want to add to the {@code headers}
     */
    public static void addHeader(Headers headers, String key, Object value) {
        notNull(headers, () -> "headers may not be null");
        if (value instanceof Instant) {
            headers.add(key, toBytes((Number) ((Instant) value).toEpochMilli()));
        } else if (value instanceof Number) {
            headers.add(key, toBytes((Number) value));
        } else if (value instanceof String) {
            headers.add(key, ((String) value).getBytes(UTF_8));
        } else if (value == null) {
            headers.add(key, null);
        } else {
            headers.add(key, value.toString().getBytes(UTF_8));
        }
    }

    /**
     * Extract the keys as a {@link Set} from the given {@code headers}.
     *
     * @param headers the Kafka {@code headers} to extract a key {@link Set} from
     * @return the keys of the given {@code headers}
     */
    public static Set<String> keys(Headers headers) {
        notNull(headers, () -> "Headers may not be null");

        return StreamSupport.stream(headers.spliterator(), false)
                            .map(Header::key)
                            .collect(Collectors.toSet());
    }

    /**
     * Generates a meta data {@code key} used to identify Axon {@link org.axonframework.messaging.MetaData} in a
     * {@link RecordHeader}.
     *
     * @param key the key to create an identifiable {@link org.axonframework.messaging.MetaData} key out of
     * @return the generated metadata key
     */
    public static String generateMetadataKey(String key) {
        return MESSAGE_METADATA + KEY_DELIMITER + key;
    }

    /**
     * Extracts the actual key name used to send over Axon {@link org.axonframework.messaging.MetaData} values.
     * E.g. from {@code 'axon-metadata-foo'} this method will extract {@code foo}.
     *
     * @param metaDataKey the generated metadata key to extract from
     * @return the extracted key
     */
    @SuppressWarnings("ConstantConditions") // Null check performed by `Assert.isTrue` call
    public static String extractKey(String metaDataKey) {
        isTrue(
                metaDataKey != null && metaDataKey.startsWith(MESSAGE_METADATA + KEY_DELIMITER),
                () -> "Cannot extract Axon MetaData key from given String [" + metaDataKey + "]"
        );

        return metaDataKey.substring((MESSAGE_METADATA + KEY_DELIMITER).length());
    }

    /**
     * Extract all Axon {@link org.axonframework.messaging.MetaData} (if any) attached in the given {@code headers}.
     * If a {@link Header#key()} matches with the {@link #isValidMetadataKey(String)} call, the given {@code header}
     * will be added to the map
     *
     * @param headers the Kafka {@link Headers} to extract a {@link org.axonframework.messaging.MetaData} map for
     * @return the map of all Axon related {@link org.axonframework.messaging.MetaData} retrieved from the given
     * {@code headers}
     */
    public static Map<String, Object> extractAxonMetadata(Headers headers) {
        notNull(headers, () -> "Headers may not be null");

        return StreamSupport.stream(headers.spliterator(), false)
                            .filter(header -> isValidMetadataKey(header.key()))
                            .collect(Collectors.toMap(
                                    header -> extractKey(header.key()),
                                    header -> asString(header.value())
                            ));
    }

    private static boolean isValidMetadataKey(String key) {
        return key.startsWith(MESSAGE_METADATA + KEY_DELIMITER);
    }

    /**
     * Generates Kafka {@link Headers} based on an {@link EventMessage} and {@link SerializedObject}, using the given
     * {@code headerValueMapper} to correctly map the values to byte arrays.
     *
     * @param eventMessage      the {@link EventMessage} to create headers for
     * @param serializedObject  the serialized payload of the given {@code eventMessage}
     * @param headerValueMapper function for converting {@code values} to bytes. Since {@link RecordHeader} can handle
     *                          only bytes this function needs to define the logic how to convert a given value to
     *                          bytes. See {@link HeaderUtils#byteMapper()} for sample implementation
     * @return the generated Kafka {@link Headers} based on an {@link EventMessage} and {@link SerializedObject}
     */
    public static Headers toHeaders(EventMessage<?> eventMessage,
                                    SerializedObject<byte[]> serializedObject,
                                    BiFunction<String, Object, RecordHeader> headerValueMapper) {
        notNull(eventMessage, () -> "EventMessage may not be null");
        notNull(serializedObject, () -> "SerializedObject may not be null");
        notNull(headerValueMapper, () -> "Header key-value mapper function may not be null");

        RecordHeaders headers = new RecordHeaders();
        eventMessage.getMetaData()
                    .forEach((k, v) -> ((Headers) headers).add(headerValueMapper.apply(generateMetadataKey(k), v)));
        defaultHeaders(eventMessage, serializedObject).forEach((k, v) -> addHeader(headers, k, v));
        return headers;
    }

    /**
     * A {@link BiFunction} that converts {@code values} to byte arrays.
     *
     * @return an {@link Object} to {@code byte[]} mapping function
     */
    public static BiFunction<String, Object, RecordHeader> byteMapper() {
        return (key, value) -> value instanceof byte[]
                ? new RecordHeader(key, (byte[]) value)
                : new RecordHeader(key, value == null ? null : value.toString().getBytes(UTF_8));
    }
}
