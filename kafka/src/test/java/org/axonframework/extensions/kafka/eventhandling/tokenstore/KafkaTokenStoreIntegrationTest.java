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

import com.thoughtworks.xstream.XStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Test class validating the {@link KafkaTokenStore}.
 *
 * @author Gerard Klijs
 */
class KafkaTokenStoreIntegrationTest extends KafkaContainerTest {

    private static final String[] TOPICS = {"__axon_token_store_updates"};
    private static final String PROCESSOR_NAME = "processor_name";
    private static final int SEGMENT = 2;

    private KafkaTokenStore tokenStore;

    @BeforeEach
    void setUp() {
        tokenStore = getTokenStore();
    }

    @AfterEach
    void tearDown() {
        tokenStore.close();
        if (!KafkaAdminUtils.listTopics(getBootstrapServers()).isEmpty()) {
            KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPICS);
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            try {
                return KafkaAdminUtils.listTopics(getBootstrapServers()).isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    void storeTokenWhenNotInitialized() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        assertThrows(UnableToClaimTokenException.class,
                     () -> tokenStore.storeToken(someToken, PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void fetchTokenWhenItsNotThere() {
        assertThrows(UnableToClaimTokenException.class, () -> tokenStore.fetchToken(PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void fetchTokenWhenNotTheOwner() {
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        tokenStore.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        assertThrows(UnableToClaimTokenException.class, () -> tokenStore.fetchToken("foo", SEGMENT));
    }

    @Test
    void requiresExplicitSegmentInitializationTest() {
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5);
        assertTrue(tokenStore.requiresExplicitSegmentInitialization());
    }

    @Test
    void deleteTokenHappyFlow() {
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        tokenStore.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        tokenStore.deleteToken(PROCESSOR_NAME, SEGMENT);
        assertEquals(4, tokenStore.fetchSegments(PROCESSOR_NAME).length);
    }

    @Test
    void deleteTokenWhenNotInitialized() {
        assertThrows(UnableToClaimTokenException.class, () -> tokenStore.deleteToken(PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void testInitializeTokensWithoutInitialToken() {
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5);

        int[] actual = tokenStore.fetchSegments(PROCESSOR_NAME);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4}, actual);
        for (int segment : actual) {
            assertNull(tokenStore.fetchToken(PROCESSOR_NAME, segment));
        }
    }

    @Test
    void testInitializeTokensWhenThereIsAlreadyASegment() {
        tokenStore.initializeSegment(null, PROCESSOR_NAME, SEGMENT);
        assertThrows(UnableToClaimTokenException.class, () -> tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5));
    }

    @Test
    void testInitializeTokensAtGivenPosition() {
        TrackingToken initialToken = new GlobalSequenceTrackingToken(1);
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5, initialToken);

        int[] actual = tokenStore.fetchSegments(PROCESSOR_NAME);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4}, actual);
        for (int segment : actual) {
            assertEquals(new GlobalSequenceTrackingToken(1), tokenStore.fetchToken(PROCESSOR_NAME, segment));
        }
    }

    @Test
    void testInitializeSegmentWithToken() {
        TrackingToken initialToken = new GlobalSequenceTrackingToken(1);
        tokenStore.initializeSegment(initialToken, PROCESSOR_NAME, SEGMENT);
        TrackingToken retrieved = tokenStore.fetchToken(PROCESSOR_NAME, SEGMENT);
        assertEquals(initialToken, retrieved);
    }

    @Test
    void testInitializeSegmentWithNull() {
        tokenStore.initializeSegment(null, PROCESSOR_NAME, SEGMENT);
        TrackingToken retrieved = tokenStore.fetchToken(PROCESSOR_NAME, SEGMENT);
        assertNull(retrieved);
    }

    @Test
    void testInitializeSegmentWhenAlreadyThere() {
        tokenStore.initializeSegment(null, PROCESSOR_NAME, SEGMENT);
        assertThrows(UnableToInitializeTokenException.class,
                     () -> tokenStore.initializeSegment(null, PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void releaseTokenWhenNotInitialized() {
        assertThrows(UnableToClaimTokenException.class, () -> tokenStore.releaseClaim(PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void testStoreTokenWhenNotInitialized() {
        TrackingToken updateToken = new GlobalSequenceTrackingToken(20);
        assertThrows(UnableToClaimTokenException.class,
                     () -> tokenStore.storeToken(updateToken, PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void testStoreTokenWhenIsInitialized() {
        tokenStore.initializeTokenSegments(PROCESSOR_NAME, 5);
        TrackingToken updateToken = new GlobalSequenceTrackingToken(20);
        tokenStore.storeToken(updateToken, PROCESSOR_NAME, SEGMENT);
        TrackingToken retrieved = tokenStore.fetchToken(PROCESSOR_NAME, SEGMENT);
        assertEquals(updateToken, retrieved);
    }

    private KafkaTokenStore getTokenStore() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        Serializer serializer = XStreamSerializer.builder()
                                                 .xStream(new XStream(new CompactDriver()))
                                                 .build();
        return KafkaTokenStore
                .builder()
                .serializer(serializer)
                .consumerConfiguration(configuration)
                .producerConfiguration(configuration)
                .build();
    }
}
