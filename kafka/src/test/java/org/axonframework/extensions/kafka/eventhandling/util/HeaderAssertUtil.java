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

package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.common.header.Headers;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.SerializedObject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;
import static org.axonframework.messaging.Headers.*;

/**
 * Utility for asserting Kafka headers sent via Axon.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public abstract class HeaderAssertUtil {

    private HeaderAssertUtil() {
        // Utility class
    }

    public static void assertEventHeaders(String metaDataKey,
                                          EventMessage<?> eventMessage,
                                          SerializedObject<byte[]> so,
                                          Headers headers) {
        assertThat(headers.toArray().length).isGreaterThanOrEqualTo(5);
        assertThat(valueAsString(headers, MESSAGE_ID)).isEqualTo(eventMessage.getIdentifier());
        assertThat(valueAsLong(headers, MESSAGE_TIMESTAMP)).isEqualTo(eventMessage.getTimestamp().toEpochMilli());
        assertThat(valueAsString(headers, MESSAGE_TYPE)).isEqualTo(so.getType().getName());
        assertThat(valueAsString(headers, MESSAGE_REVISION)).isEqualTo(so.getType().getRevision());
        assertThat(valueAsString(headers, generateMetadataKey(metaDataKey)))
                .isEqualTo(eventMessage.getMetaData().get(metaDataKey));
    }

    public static void assertDomainHeaders(DomainEventMessage<?> eventMessage, Headers headers) {
        assertThat(headers.toArray().length).isGreaterThanOrEqualTo(8);
        assertThat(valueAsLong(headers, AGGREGATE_SEQ)).isEqualTo(eventMessage.getSequenceNumber());
        assertThat(valueAsString(headers, AGGREGATE_ID)).isEqualTo(eventMessage.getAggregateIdentifier());
        assertThat(valueAsString(headers, AGGREGATE_TYPE)).isEqualTo(eventMessage.getType());
    }
}
