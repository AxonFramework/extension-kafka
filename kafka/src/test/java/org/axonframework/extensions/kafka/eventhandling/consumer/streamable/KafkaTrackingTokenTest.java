/*
 * Copyright (c) 2010-2019. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests validating the {@link KafkaTrackingToken}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class KafkaTrackingTokenTest {

    private static final String TEST_TOPIC = "topic";
    private static final int TEST_PARTITION = 0;
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    @Test
    void testNullTokenShouldBeEmpty() {
        //noinspection ConstantConditions
        assertTrue(isEmpty(null));
    }

    @Test
    void testTokensWithoutPartitionsShouldBeEmpty() {
        assertTrue(isEmpty(emptyToken()));
    }

    @Test
    void testTokensWithPartitionsShouldBeEmpty() {
        assertTrue(isNotEmpty(nonEmptyToken()));
    }

    @Test
    void testAdvanceToInvalidTopic() {
        assertThrows(IllegalArgumentException.class, () -> emptyToken().advancedTo("", 0, 1));
    }

    @Test
    void testAdvanceToInvalidPartition() {
        assertThrows(IllegalArgumentException.class, () -> emptyToken().advancedTo(TEST_TOPIC, -1, 1));
    }

    @Test
    void testAdvanceToInvalidOffset() {
        assertThrows(IllegalArgumentException.class, () -> emptyToken().advancedTo(TEST_TOPIC, 0, -1));
    }

    @Test
    void testCompareTokens() {
        KafkaTrackingToken original = nonEmptyToken();
        KafkaTrackingToken copy = newInstance(singletonMap(TEST_TOPIC_PARTITION, 0L));
        KafkaTrackingToken forge = newInstance(singletonMap(TEST_TOPIC_PARTITION, 1L));

        assertEquals(original, original);
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertNotEquals(forge, original);
        assertNotEquals(forge.hashCode(), original.hashCode());
        assertNotEquals("foo", original);
    }

    @Test
    void testTokenIsHumanReadable() {
        assertEquals("KafkaTrackingToken{positions={topic-0=0}}", nonEmptyToken().toString());
    }

    private static KafkaTrackingToken nonEmptyToken() {
        return newInstance(singletonMap(TEST_TOPIC_PARTITION, 0L));
    }

    @Test
    void testAdvanceToLaterTimestamp() {
        Map<TopicPartition, Long> testPositions = new HashMap<>();
        testPositions.put(TEST_TOPIC_PARTITION, 0L);
        testPositions.put(new TopicPartition(TEST_TOPIC, 1), 0L);
        KafkaTrackingToken testToken = newInstance(testPositions);

        KafkaTrackingToken testSubject = testToken.advancedTo(TEST_TOPIC, TEST_PARTITION, 1L);

        assertNotSame(testSubject, testToken);
        assertEquals(Long.valueOf(1), testSubject.positions().get(TEST_TOPIC_PARTITION));
        assertKnownEventIds(testSubject, TEST_TOPIC_PARTITION, new TopicPartition(TEST_TOPIC, 1));
    }

    @Test
    void testAdvanceToOlderTimestamp() {
        Map<TopicPartition, Long> testPositions = new HashMap<>();
        testPositions.put(TEST_TOPIC_PARTITION, 1L);
        testPositions.put(new TopicPartition(TEST_TOPIC, 1), 0L);
        KafkaTrackingToken testToken = newInstance(testPositions);

        KafkaTrackingToken testSubject = testToken.advancedTo(TEST_TOPIC, TEST_PARTITION, 0L);

        assertNotSame(testSubject, testToken);
        assertEquals(Long.valueOf(0), testSubject.positions().get(TEST_TOPIC_PARTITION));
        assertKnownEventIds(testSubject, TEST_TOPIC_PARTITION, new TopicPartition(TEST_TOPIC, 1));
    }

    private static void assertKnownEventIds(KafkaTrackingToken token, TopicPartition... expectedKnownIds) {
        assertEquals(
                Stream.of(expectedKnownIds).collect(toSet()),
                new HashSet<>(token.positions().keySet())
        );
    }

    @Test
    void testAdvancingATokenMakesItCoverThePrevious() {
        KafkaTrackingToken testSubject = newInstance(singletonMap(TEST_TOPIC_PARTITION, 0L));
        KafkaTrackingToken advancedToken = testSubject.advancedTo(TEST_TOPIC, TEST_PARTITION, 1);
        KafkaTrackingToken uncoveredToken =
                newInstance(singletonMap(new TopicPartition(TEST_TOPIC, 1), 0L));

        assertTrue(advancedToken.covers(testSubject));
        assertFalse(uncoveredToken.covers(testSubject));
    }

    @Test
    void testUpperBound() {
        Map<TopicPartition, Long> firstPositions = new HashMap<>();
        firstPositions.put(new TopicPartition(TEST_TOPIC, 0), 0L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 1), 10L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 3), 2L);
        KafkaTrackingToken first = newInstance(firstPositions);

        Map<TopicPartition, Long> secondPositions = new HashMap<>();
        secondPositions.put(new TopicPartition(TEST_TOPIC, 0), 10L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 1), 1L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 4), 3L);
        KafkaTrackingToken second = newInstance(secondPositions);

        Map<TopicPartition, Long> expectedPositions = new HashMap<>();
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 0), 10L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 1), 10L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 3), 2L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 4), 3L);
        KafkaTrackingToken expected = newInstance(expectedPositions);

        assertEquals(expected, first.upperBound(second));
    }

    @Test
    void testLowerBound() {
        Map<TopicPartition, Long> firstPositions = new HashMap<>();
        firstPositions.put(new TopicPartition(TEST_TOPIC, 0), 0L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 1), 10L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        firstPositions.put(new TopicPartition(TEST_TOPIC, 3), 2L);
        KafkaTrackingToken first = newInstance(firstPositions);

        Map<TopicPartition, Long> secondPositions = new HashMap<>();
        secondPositions.put(new TopicPartition(TEST_TOPIC, 0), 10L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 1), 1L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        secondPositions.put(new TopicPartition(TEST_TOPIC, 4), 3L);
        KafkaTrackingToken second = newInstance(secondPositions);

        Map<TopicPartition, Long> expectedPositions = new HashMap<>();
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 0), 0L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 1), 1L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 2), 2L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 3), 0L);
        expectedPositions.put(new TopicPartition(TEST_TOPIC, 4), 0L);
        KafkaTrackingToken expected = newInstance(expectedPositions);

        assertEquals(expected, first.lowerBound(second));
    }

    @Test
    void testCoversRegression() {
        KafkaTrackingToken first = newInstance(singletonMap(new TopicPartition(TEST_TOPIC, 1), 2L));
        KafkaTrackingToken second = newInstance(singletonMap(new TopicPartition(TEST_TOPIC, 0), 1L));

        assertFalse(first.covers(second));
    }

    @Test
    void testRelationOfCoversAndUpperBounds() {
        Random random = new Random();
        for (int i = 0; i <= 10; i++) {
            Map<TopicPartition, Long> firstPositions = new HashMap<>();
            firstPositions.put(new TopicPartition(TEST_TOPIC, random.nextInt(2)), (long) random.nextInt(4) + 1);
            firstPositions.put(new TopicPartition(TEST_TOPIC, random.nextInt(2)), (long) random.nextInt(4) + 1);
            KafkaTrackingToken first = newInstance(firstPositions);

            Map<TopicPartition, Long> secondPositions = new HashMap<>();
            secondPositions.put(new TopicPartition(TEST_TOPIC, random.nextInt(2)), (long) random.nextInt(4) + 1);
            secondPositions.put(new TopicPartition(TEST_TOPIC, random.nextInt(2)), (long) random.nextInt(4) + 1);
            KafkaTrackingToken second = newInstance(secondPositions);

            if (first.upperBound(second).equals(first)) {
                assertTrue(first.covers(second),
                           "The upper bound of first[" + first + "] and second[" + second
                                   + "] is not first, so first shouldn't cover second");
            } else {
                assertFalse(first.covers(second),
                            "The upper bound of first[" + first + "] and second[" + second
                                    + "] is not first, so first shouldn't cover second");
            }
        }
    }
}
