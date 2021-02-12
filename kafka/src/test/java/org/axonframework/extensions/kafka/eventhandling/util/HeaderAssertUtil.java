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

import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;
import static org.axonframework.messaging.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

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
        assertTrue(headers.toArray().length >= 5);
        assertEquals(eventMessage.getIdentifier(), valueAsString(headers, MESSAGE_ID));
        assertEquals(eventMessage.getTimestamp().toEpochMilli(), valueAsLong(headers, MESSAGE_TIMESTAMP));
        assertEquals(so.getType().getName(), valueAsString(headers, MESSAGE_TYPE));
        assertEquals(so.getType().getRevision(), valueAsString(headers, MESSAGE_REVISION));
        assertEquals(eventMessage.getMetaData().get(metaDataKey),
                     valueAsString(headers, generateMetadataKey(metaDataKey)));
    }

    public static void assertDomainHeaders(DomainEventMessage<?> eventMessage, Headers headers) {
        assertTrue(headers.toArray().length >= 8);
        assertEquals(eventMessage.getSequenceNumber(), valueAsLong(headers, AGGREGATE_SEQ));
        assertEquals(eventMessage.getAggregateIdentifier(), valueAsString(headers, AGGREGATE_ID));
        assertEquals(eventMessage.getType(), valueAsString(headers, AGGREGATE_TYPE));
    }
}
