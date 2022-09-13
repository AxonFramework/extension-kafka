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
import org.axonframework.eventhandling.EventMessage;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

/**
 * Utility class for dealing with cloud events conversion, to store and retrieve data from/to Metadata.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class MetadataUtils {

    /**
     * Metadata key to store the subject.
     */
    static final String SUBJECT = "cloud-event-subject";
    /**
     * Metadata key to store the data content type.
     */
    static final String DATA_CONTENT_TYPE = "cloud-event-data-content-type";
    /**
     * Metadata key to store the data schema.
     */
    static final String DATA_SCHEMA = "cloud-event-data-schema";
    private static final Set<String> RESERVED_METADATA = Stream
            .of(SUBJECT, DATA_CONTENT_TYPE, DATA_SCHEMA)
            .collect(Collectors.toCollection(HashSet::new));

    private MetadataUtils() {
        // Utility class
    }

    static Predicate<Map.Entry<String, Object>> reservedMetadataFilter() {
        return e -> !RESERVED_METADATA.contains(e.getKey());
    }

    static Map<String, Object> getAdditionalEntries(CloudEvent cloudEvent) {
        Map<String, Object> metadataMap = new HashMap<>();
        if (!isNull(cloudEvent.getSubject())) {
            metadataMap.put(SUBJECT, cloudEvent.getSubject());
        }
        if (!isNull(cloudEvent.getDataContentType())) {
            metadataMap.put(DATA_CONTENT_TYPE, cloudEvent.getDataContentType());
        }
        if (!isNull(cloudEvent.getDataSchema())) {
            metadataMap.put(DATA_SCHEMA, cloudEvent.getDataSchema());
        }
        return metadataMap;
    }

    @SuppressWarnings("squid:S1452") //needs wildcard to be generic
    static Function<EventMessage<?>, Optional<String>> defaultSubjectSupplier() {
        return message -> {
            Object subject = message.getMetaData().get(SUBJECT);
            if (subject instanceof String) {
                return Optional.of((String) subject);
            } else {
                return Optional.empty();
            }
        };
    }

    @SuppressWarnings("squid:S1452") //needs wildcard to be generic
    static Function<EventMessage<?>, Optional<String>> defaultDataContentTypeSupplier() {
        return message -> {
            Object dataContentType = message.getMetaData().get(DATA_CONTENT_TYPE);
            if (dataContentType instanceof String) {
                return Optional.of((String) dataContentType);
            } else {
                return Optional.empty();
            }
        };
    }

    @SuppressWarnings("squid:S1452") //needs wildcard to be generic
    static Function<EventMessage<?>, Optional<URI>> defaultDataSchemaSupplier() {
        return message -> {
            Object dataSchema = message.getMetaData().get(DATA_SCHEMA);
            if (dataSchema instanceof URI) {
                return Optional.of((URI) dataSchema);
            } else {
                return Optional.empty();
            }
        };
    }
}
