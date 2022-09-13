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
import io.cloudevents.CloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static org.axonframework.extensions.kafka.eventhandling.cloudevent.MetadataUtils.reservedMetadataFilter;

/**
 * Utility class for dealing with cloud event extension, to store and retrieve data.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
class ExtensionUtils {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionUtils.class);

    /**
     * Extension name pointing to the revision of a message.
     */
    static final String MESSAGE_REVISION = "axonmessagerevision";
    /**
     * Extension name pointing to the aggregate identifier of a message.
     */
    static final String AGGREGATE_ID = "axonmessageaggregateid";
    /**
     * Extension name pointing to the aggregate sequence of a message.
     */
    static final String AGGREGATE_SEQ = "axonmessageaggregateseq";
    /**
     * Extension name pointing to the aggregate type of a message.
     */
    static final String AGGREGATE_TYPE = "axonmessageaggregatetype";

    private static final Set<String> NON_METADATA_EXTENSIONS = Stream
            .of(AGGREGATE_TYPE, AGGREGATE_ID, AGGREGATE_SEQ, MESSAGE_REVISION)
            .collect(Collectors.toCollection(HashSet::new));

    private ExtensionUtils() {
        // Utility class
    }

    static void setExtensions(
            CloudEventBuilder builder,
            EventMessage<?> message,
            SerializedObject<byte[]> serializedObject,
            Map<String, String> extensionNameResolver
    ) {
        if (!isNull(serializedObject.getType().getRevision())) {
            builder.withExtension(MESSAGE_REVISION, serializedObject.getType().getRevision());
        }
        if (message instanceof DomainEventMessage) {
            DomainEventMessage<?> domainMessage = (DomainEventMessage<?>) message;
            builder.withExtension(AGGREGATE_ID, domainMessage.getAggregateIdentifier());
            builder.withExtension(AGGREGATE_SEQ, domainMessage.getSequenceNumber());
            builder.withExtension(AGGREGATE_TYPE, domainMessage.getType());
        }
        message.getMetaData().entrySet()
               .stream()
               .filter(reservedMetadataFilter())
               .forEach(entry -> setExtension(builder,
                                              resolveExtensionName(entry.getKey(), extensionNameResolver),
                                              entry.getValue()));
    }

    static MetaData getExtensionsAsMetadata(CloudEvent cloudEvent, Map<String, String> metadataNameResolver) {
        Map<String, Object> metadataMap = new HashMap<>();
        cloudEvent.getExtensionNames().forEach(name -> {
            if (!isNonMetadataExtension(name)) {
                metadataMap.put(resolveMetadataKey(name, metadataNameResolver), cloudEvent.getExtension(name));
            }
        });
        return MetaData.from(metadataMap);
    }

    private static String resolveMetadataKey(String extensionName, Map<String, String> metadataNameResolver) {
        if (metadataNameResolver.containsKey(extensionName)) {
            return metadataNameResolver.get(extensionName);
        }
        logger.debug("Extension name: '{}' was not part of the supplied map, this might give errors", extensionName);
        return extensionName;
    }

    private static String resolveExtensionName(String metadataKey, Map<String, String> extensionNameResolver) {
        if (extensionNameResolver.containsKey(metadataKey)) {
            return extensionNameResolver.get(metadataKey);
        }
        logger.debug("Metadata key: '{}' was not part of the supplied map, this might give errors", metadataKey);
        return metadataKey;
    }

    static boolean isNonMetadataExtension(String extensionName) {
        return NON_METADATA_EXTENSIONS.contains(extensionName);
    }

    static void setExtension(CloudEventBuilder builder, String extensionName, Object value) {
        if (isNonMetadataExtension(extensionName)) {
            throw new InvalidMetaDataException(
                    String.format("Metadata property '%s' is already reserved to be used for Axon",
                                  extensionName)
            );
        }
        if (!isValidExtensionName(extensionName)) {
            throw new InvalidMetaDataException(
                    String.format("Metadata property '%s' is not a valid extension name",
                                  extensionName)
            );
        }
        if (value instanceof String) {
            builder.withExtension(extensionName, (String) value);
        } else if (value instanceof Number) {
            builder.withExtension(extensionName, (Number) value);
        } else if (value instanceof Boolean) {
            builder.withExtension(extensionName, (Boolean) value);
        } else if (value instanceof URI) {
            builder.withExtension(extensionName, (URI) value);
        } else if (value instanceof OffsetDateTime) {
            builder.withExtension(extensionName, (OffsetDateTime) value);
        } else if (value instanceof byte[]) {
            builder.withExtension(extensionName, (byte[]) value);
        } else {
            throw new InvalidMetaDataException(
                    String.format("Metadata property '%s' is of class '%s' and thus can't be added",
                                  extensionName,
                                  value.getClass())
            );
        }
    }

    static String asNullableString(Object object) {
        if (object instanceof String) {
            return (String) object;
        } else {
            return null;
        }
    }

    static Long asLong(Object object) {
        if (object instanceof Long) {
            return (Long) object;
        } else {
            return 0L;
        }
    }

    static OffsetDateTime asOffsetDateTime(Object object, long fallbackTimestamp) {
        if (object instanceof OffsetDateTime) {
            return (OffsetDateTime) object;
        } else {
            return Instant.ofEpochMilli(fallbackTimestamp).atOffset(ZoneOffset.UTC);
        }
    }

    static byte[] asBytes(CloudEventData data) {
        if (isNull(data)) {
            return new byte[0];
        } else {
            return data.toBytes();
        }
    }

    static boolean isValidExtensionName(String name) {
        for (int i = 0; i < name.length(); ++i) {
            if (!isValidChar(name.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    private static boolean isValidChar(char c) {
        return c >= 'a' && c <= 'z' || c >= '0' && c <= '9';
    }

    static boolean isValidMetadataToExtensionMap(Map<String, String> metadataToExtensionMap) {
        for (String extensionName : metadataToExtensionMap.values()) {
            if (!isValidExtensionName(extensionName)) {
                return false;
            }
        }
        return true;
    }
}
