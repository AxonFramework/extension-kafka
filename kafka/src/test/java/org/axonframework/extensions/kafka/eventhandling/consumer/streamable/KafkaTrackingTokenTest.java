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
import org.assertj.core.util.Lists;
import org.junit.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotSame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken.*;

/**
 * Tests for the {@link KafkaTrackingToken}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public class KafkaTrackingTokenTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNullTokenShouldBeEmpty() {
        assertThat(isEmpty(null)).isTrue();
    }

    @Test
    public void testTokensWithoutPartitionsShouldBeEmpty() {
        assertThat(isEmpty(emptyToken())).isTrue();
    }

    @Test
    public void testTokensWithPartitionsShouldBeEmpty() {
        assertThat(isNotEmpty(nonEmptyToken())).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdvanceToInvalidPartition() {
        KafkaTrackingToken.newInstance(Collections.emptyMap()).advancedTo(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAdvanceToInvalidOffset() {
        KafkaTrackingToken.newInstance(Collections.emptyMap()).advancedTo(0, -1);
    }

    @Test
    public void testCompareTokens() {
        KafkaTrackingToken original = nonEmptyToken();
        KafkaTrackingToken copy = KafkaTrackingToken.newInstance(Collections.singletonMap(0, 0L));
        KafkaTrackingToken forge = KafkaTrackingToken.newInstance(Collections.singletonMap(0, 1L));

        assertThat(original).isEqualTo(original);
        assertThat(copy).isEqualTo(original);
        assertThat(copy.hashCode()).isEqualTo(original.hashCode());
        assertThat(original).isNotEqualTo(forge);
        assertThat(original.hashCode()).isNotEqualTo(forge.hashCode());
        assertThat(original).isNotEqualTo("foo");
    }

    @Test
    public void testTokenIsHumanReadable() {
        assertThat(nonEmptyToken().toString()).isEqualTo("KafkaTrackingToken{partitionPositions={0=0}}");
    }

    @Test
    public void testAdvanceToLaterTimestamp() {
        Map<Integer, Long> testPartitionPositions = new HashMap<>();
        testPartitionPositions.put(0, 0L);
        testPartitionPositions.put(1, 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(testPartitionPositions);

        KafkaTrackingToken testSubject = testToken.advancedTo(0, 1L);

        assertNotSame(testSubject, testToken);
        assertEquals(Long.valueOf(1), testSubject.partitionPositions().get(0));
        assertKnownEventIds(testSubject, 0, 1);
    }

    @Test
    public void testAdvanceToOlderTimestamp() {
        Map<Integer, Long> testPartitionPositions = new HashMap<>();
        testPartitionPositions.put(0, 1L);
        testPartitionPositions.put(1, 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(testPartitionPositions);

        KafkaTrackingToken testSubject = testToken.advancedTo(0, 0L);

        assertNotSame(testSubject, testToken);
        assertEquals(Long.valueOf(0), testSubject.partitionPositions().get(0));
        assertKnownEventIds(testSubject, 0, 1);
    }

    @Test
    public void testTopicPartitionCreation() {
        assertEquals(new TopicPartition("foo", 0), KafkaTrackingToken.partition("foo", 0));
    }

    @Test
    public void testRestoringKafkaPartitionsFromExistingToken() {
        Map<Integer, Long> testPartitionPositions = new HashMap<>();
        testPartitionPositions.put(0, 0L);
        testPartitionPositions.put(2, 10L);

        KafkaTrackingToken existingToken = KafkaTrackingToken.newInstance(testPartitionPositions);

        Collection<TopicPartition> expected =
                Lists.newArrayList(new TopicPartition("bar", 0), new TopicPartition("bar", 2));
        assertEquals(expected, existingToken.partitions("bar"));
    }

    @Test
    public void testAdvancingATokenMakesItCoverThePrevious() {
        KafkaTrackingToken testSubject = KafkaTrackingToken.newInstance(Collections.singletonMap(0, 0L));
        KafkaTrackingToken advancedToken = testSubject.advancedTo(0, 1);
        assertThat(advancedToken.covers(testSubject)).isTrue();
        assertThat(KafkaTrackingToken.newInstance(Collections.singletonMap(1, 0L)).covers(testSubject)).isFalse();
    }

    @Test
    public void testUpperBound() {
        Map<Integer, Long> firstPartitionPositions = new HashMap<>();
        firstPartitionPositions.put(0, 0L);
        firstPartitionPositions.put(1, 10L);
        firstPartitionPositions.put(2, 2L);
        firstPartitionPositions.put(3, 2L);
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(firstPartitionPositions);

        Map<Integer, Long> secondPartitionPositions = new HashMap<>();
        secondPartitionPositions.put(0, 10L);
        secondPartitionPositions.put(1, 1L);
        secondPartitionPositions.put(2, 2L);
        secondPartitionPositions.put(4, 3L);
        KafkaTrackingToken second = KafkaTrackingToken.newInstance(secondPartitionPositions);

        Map<Integer, Long> expectedPartitionPositions = new HashMap<>();
        expectedPartitionPositions.put(0, 10L);
        expectedPartitionPositions.put(1, 10L);
        expectedPartitionPositions.put(2, 2L);
        expectedPartitionPositions.put(3, 2L);
        expectedPartitionPositions.put(4, 3L);
        KafkaTrackingToken expected = KafkaTrackingToken.newInstance(expectedPartitionPositions);

        assertThat(first.upperBound(second)).isEqualTo(expected);
    }

    @Test
    public void testLowerBound() {
        Map<Integer, Long> firstPartitionPositions = new HashMap<>();
        firstPartitionPositions.put(0, 0L);
        firstPartitionPositions.put(1, 10L);
        firstPartitionPositions.put(2, 2L);
        firstPartitionPositions.put(3, 2L);
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(firstPartitionPositions);

        Map<Integer, Long> secondPartitionPositions = new HashMap<>();
        secondPartitionPositions.put(0, 10L);
        secondPartitionPositions.put(1, 1L);
        secondPartitionPositions.put(2, 2L);
        secondPartitionPositions.put(4, 3L);
        KafkaTrackingToken second = KafkaTrackingToken.newInstance(secondPartitionPositions);

        Map<Integer, Long> expectedPartitionPositions = new HashMap<>();
        expectedPartitionPositions.put(0, 0L);
        expectedPartitionPositions.put(1, 1L);
        expectedPartitionPositions.put(2, 2L);
        expectedPartitionPositions.put(3, 0L);
        expectedPartitionPositions.put(4, 0L);
        KafkaTrackingToken expected = KafkaTrackingToken.newInstance(expectedPartitionPositions);

        assertThat(first.lowerBound(second)).isEqualTo(expected);
    }

    @Test
    public void testCoversRegression() {
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(Collections.singletonMap(1, 2L));
        KafkaTrackingToken second = KafkaTrackingToken.newInstance(Collections.singletonMap(0, 1L));

        assertThat(first.covers(second)).isFalse();
    }

    @Test
    public void testRelationOfCoversAndUpperBounds() {
        Random random = new Random();
        for (int i = 0; i <= 10; i++) {
            Map<Integer, Long> firstPartitionPositions = new HashMap<>();
            firstPartitionPositions.put(random.nextInt(2), (long) random.nextInt(4) + 1);
            firstPartitionPositions.put(random.nextInt(2), (long) random.nextInt(4) + 1);
            KafkaTrackingToken first = KafkaTrackingToken.newInstance(firstPartitionPositions);

            Map<Integer, Long> secondPartitionPositions = new HashMap<>();
            secondPartitionPositions.put(random.nextInt(2), (long) random.nextInt(4) + 1);
            secondPartitionPositions.put(random.nextInt(2), (long) random.nextInt(4) + 1);
            KafkaTrackingToken second = KafkaTrackingToken.newInstance(secondPartitionPositions);

            if (first.upperBound(second).equals(first)) {
                assertThat(first.covers(second))
                        .withFailMessage(
                                "The upper bound of first(%s) and second(%s) is not first, so first shouldn't cover second",
                                first,
                                second)
                        .isTrue();
            } else {
                assertThat(first.covers(second))
                        .withFailMessage(
                                "The upper bound of first(%s) and second(%s) is not first, so first shouldn't cover second",
                                first,
                                second)
                        .isFalse();
            }
        }
    }

    private static KafkaTrackingToken nonEmptyToken() {
        return KafkaTrackingToken.newInstance(Collections.singletonMap(0, 0L));
    }

    private static void assertKnownEventIds(KafkaTrackingToken token, Integer... expectedKnownIds) {
        assertEquals(Stream.of(expectedKnownIds).collect(toSet()),
                     new HashSet<>(token.partitionPositions().keySet()));
    }
}
