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

import io.cloudevents.CloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

/**
 * Utility class for dealing with cloud event extension, to store and retrieve data.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
class ExtensionUtils {

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

    static boolean isNonMetadataExtension(String extensionName) {
        return NON_METADATA_EXTENSIONS.contains(extensionName);
    }

    static void setExtension(CloudEventBuilder builder, Map.Entry<String, Object> entry) {
        if (isNonMetadataExtension(entry.getKey())) {
            throw new InvalidMetaDataException(
                    String.format("Metadata property '%s' is already reserved to be used for Axon",
                                  entry.getKey())
            );
        }
        if (entry.getValue() instanceof String) {
            builder.withExtension(entry.getKey(), (String) entry.getValue());
        } else if (entry.getValue() instanceof Number) {
            builder.withExtension(entry.getKey(), (Number) entry.getValue());
        } else if (entry.getValue() instanceof Boolean) {
            builder.withExtension(entry.getKey(), (Boolean) entry.getValue());
        } else if (entry.getValue() instanceof URI) {
            builder.withExtension(entry.getKey(), (URI) entry.getValue());
        } else if (entry.getValue() instanceof OffsetDateTime) {
            builder.withExtension(entry.getKey(), (OffsetDateTime) entry.getValue());
        } else if (entry.getValue() instanceof byte[]) {
            builder.withExtension(entry.getKey(), (byte[]) entry.getValue());
        } else {
            throw new InvalidMetaDataException(
                    String.format("Metadata property '%s' is of class '%s' and thus can't be added",
                                  entry.getKey(),
                                  entry.getValue().getClass())
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

    static OffsetDateTime asOffsetDateTime(Object object) {
        if (object instanceof OffsetDateTime) {
            return (OffsetDateTime) object;
        } else {
            return Instant.now().atOffset(ZoneOffset.UTC);
        }
    }

    static byte[] asBytes(CloudEventData data) {
        if (isNull(data)) {
            return new byte[0];
        } else {
            return data.toBytes();
        }
    }
}
