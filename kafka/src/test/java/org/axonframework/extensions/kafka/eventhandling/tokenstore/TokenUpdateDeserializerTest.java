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

package org.axonframework.extensions.kafka.eventhandling.tokenstore;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.axonframework.extensions.kafka.eventhandling.HeaderUtils;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link TokenUpdateSerializer}.
 *
 * @author Gerard Klijs
 */
class TokenUpdateDeserializerTest {

    @Test
    void testDeserializeUnsupportedOperation() {
        TokenUpdateDeserializer deserializer = new TokenUpdateDeserializer();
        byte[] bytes = new byte[0];
        assertThrows(UnsupportedOperationException.class, () -> deserializer.deserialize("topic", bytes));
    }

    @Test
    void testDeserializerMostlyEmpty() {
        TokenUpdateDeserializer deserializer = new TokenUpdateDeserializer();
        byte[] bytes = new byte[0];
        Headers headers = new RecordHeaders();
        HeaderUtils.addHeader(headers, "id", UUID.randomUUID());
        Instant now = Instant.now();
        HeaderUtils.addHeader(headers, "timestamp", now.toEpochMilli());
        TokenUpdate update = deserializer.deserialize("topic", headers, bytes);
        assertNotNull(update);
        assertEquals(now.toEpochMilli(), update.getTimestamp().toEpochMilli());
    }
}
