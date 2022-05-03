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
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Test class validating the {@link KafkaTokenStore} that need two stores.
 *
 * @author Gerard Klijs
 */
class KafkaTokenStoreConcurrentIntegrationTest extends KafkaContainerTest {

    private static final String[] TOPICS = {"some_other_topic"};
    private static final String PROCESSOR_NAME = "processor_name";
    private static final int SEGMENT = 2;

    private KafkaTokenStore t1;
    private KafkaTokenStore t2;

    @BeforeEach
    void setUp() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TOPICS);
        t1 = getTokenStore("node-1");
        t2 = getTokenStore("node-2");
        t1.initializeTokenSegments(PROCESSOR_NAME, 5);
    }

    @AfterEach
    void tearDown() {
        t1.close();
        t2.close();
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPICS);
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            try {
                return KafkaAdminUtils.listTopics(getBootstrapServers()).isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    void storeTokenConcurrently() throws InterruptedException {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        testConcurrency(
                () -> {
                    t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
                    return true;
                },
                () -> {
                    t2.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
                    return true;
                });
    }

    @Test
    void deleteTokenConcurrently() throws InterruptedException {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        testConcurrency(
                () -> {
                    t1.deleteToken(PROCESSOR_NAME, SEGMENT);
                    return true;
                },
                () -> {
                    t2.deleteToken(PROCESSOR_NAME, SEGMENT);
                    return true;
                });
    }

    @Test
    void fetchTokenConcurrently() throws InterruptedException {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        t1.releaseClaim(PROCESSOR_NAME, SEGMENT);
        TrackingToken result = testConcurrency(
                () -> t1.fetchToken(PROCESSOR_NAME, SEGMENT),
                () -> t2.fetchToken(PROCESSOR_NAME, SEGMENT)
        );
        assertEquals(someToken, result);
    }

    @Test
    void initializeSegmentWithTokenConcurrently() throws InterruptedException {
        TrackingToken initialToken = new GlobalSequenceTrackingToken(42);
        testConcurrency(
                () -> {
                    t1.initializeSegment(initialToken, PROCESSOR_NAME, 5);
                    return true;
                },
                () -> {
                    t2.initializeSegment(initialToken, PROCESSOR_NAME, 5);
                    return true;
                });
    }

    @Test
    void releaseClaimWorking() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        t1.releaseClaim(PROCESSOR_NAME, SEGMENT);
        AtomicReference<TrackingToken> result = new AtomicReference<>(null);
        await().atMost(Duration.ofMillis(300L)).until(() -> {
            try {
                result.set(t2.fetchToken(PROCESSOR_NAME, SEGMENT));
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(someToken, result.get());
    }

    @Test
    void releaseTokenWhenNotTheOwner() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        assertThrows(UnableToClaimTokenException.class, () -> t2.releaseClaim(PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void deleteTokenWhenNotTheOwner() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        assertThrows(UnableToClaimTokenException.class, () -> t2.deleteToken(PROCESSOR_NAME, SEGMENT));
    }

    @Test
    void fetchAvailableSegmentsWorking() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, 0);
        t1.storeToken(someToken, PROCESSOR_NAME, 1);
        t2.storeToken(someToken, PROCESSOR_NAME, 2);
        t2.storeToken(someToken, PROCESSOR_NAME, 3);
        await().atMost(Duration.ofSeconds(2)).until(() -> {
            try {
                List<Integer> actual = t1.fetchAvailableSegments(PROCESSOR_NAME).stream()
                                         .map(Segment::getSegmentId)
                                         .sorted()
                                         .collect(Collectors.toList());
                assertEquals(Arrays.asList(0, 1, 4), actual);
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        await().atMost(Duration.ofSeconds(2)).until(() -> {
            try {
                List<Integer> actual = t2.fetchAvailableSegments(PROCESSOR_NAME).stream()
                                         .map(Segment::getSegmentId)
                                         .sorted()
                                         .collect(Collectors.toList());
                assertEquals(Arrays.asList(2, 3, 4), actual);
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    void ableToClaimAfterSetClaimTimeout() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        AtomicReference<TrackingToken> result = new AtomicReference<>(null);
        await().atMost(Duration.ofSeconds(2)).until(() -> {
            try {
                result.set(t2.fetchToken(PROCESSOR_NAME, SEGMENT));
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(someToken, result.get());
    }

    @Test
    void ableToStoreAfterSetClaimTimeout() {
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        t1.storeToken(someToken, PROCESSOR_NAME, SEGMENT);
        TrackingToken otherToken = new GlobalSequenceTrackingToken(43);
        AtomicReference<TrackingToken> result = new AtomicReference<>(null);
        await().atMost(Duration.ofSeconds(2)).until(() -> {
            try {
                t2.storeToken(otherToken, PROCESSOR_NAME, SEGMENT);
                result.set(t2.fetchToken(PROCESSOR_NAME, SEGMENT));
                return true;
            } catch (Exception e) {
                return false;
            }
        });
        assertEquals(otherToken, result.get());
    }

    private KafkaTokenStore getTokenStore(String nodeId) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        Serializer serializer = XStreamSerializer.builder()
                                                 .xStream(new XStream(new CompactDriver()))
                                                 .build();
        KafkaTokenStore tokenStore = KafkaTokenStore
                .builder()
                .topic(TOPICS[0])
                .nodeId(nodeId)
                .claimTimeout(Duration.ofSeconds(1))
                .serializer(serializer)
                .consumerConfiguration(configuration)
                .producerConfiguration(configuration)
                .readTimeOut(Duration.ofSeconds(10L))
                .writeTimeout(Duration.ofSeconds(10L))
                .build();
        tokenStore.start();
        return tokenStore;
    }

    private <T> T testConcurrency(Supplier<T> s1, Supplier<T> s2) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<T> r1 = new AtomicReference<>(null);
        AtomicReference<T> r2 = new AtomicReference<>(null);
        executor.execute(() -> r1.set(s1.get()));
        executor.execute(() -> r2.set(s2.get()));
        executor.shutdown();
        boolean done = executor.awaitTermination(6L, TimeUnit.SECONDS);
        assertTrue(done, "should complete in 6 seconds");
        if (r1.get() == null) {
            assertNotNull(r2.get(), "at least one of the results should be valid");
            return r2.get();
        } else {
            assertNull(r2.get(), "only one of the results should be valid");
            return r1.get();
        }
    }
}
