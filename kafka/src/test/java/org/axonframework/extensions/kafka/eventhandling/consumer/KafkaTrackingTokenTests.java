/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotSame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.emptyToken;
import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.isEmpty;
import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.isNotEmpty;

/**
 * Tests for {@link KafkaTrackingToken}
 *
 * @author Nakul Mishra
 */
public class KafkaTrackingTokenTests {

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

        KafkaTrackingToken copy = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
        }});
        KafkaTrackingToken forge = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 1L);
        }});

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
        KafkaTrackingToken start = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
            put(1, 0L);
        }});
        KafkaTrackingToken subject = start.advancedTo(0, 1L);
        assertNotSame(subject, start);
        assertEquals(Long.valueOf(1), subject.partitionPositions().get(0));
        assertKnownEventIds(subject, 0, 1);
    }

    @Test
    public void testAdvanceToOlderTimestamp() {
        KafkaTrackingToken start = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 1L);
            put(1, 0L);
        }});
        KafkaTrackingToken subject = start.advancedTo(0, 0L);
        assertNotSame(subject, start);
        assertEquals(Long.valueOf(0), subject.partitionPositions().get(0));
        assertKnownEventIds(subject, 0, 1);
    }

    @Test
    public void testTopicPartitionCreation() {
        assertEquals(new TopicPartition("foo", 0), KafkaTrackingToken.partition("foo", 0));
    }

    @Test
    public void testRestoringKafkaPartitionsFromExistingToken() {
        KafkaTrackingToken existingToken = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
            put(2, 10L);
        }});
        Collection<TopicPartition> expected = Lists.newArrayList(new TopicPartition("bar", 0),
                                                                 new TopicPartition("bar", 2));
        assertEquals(expected, existingToken.partitions("bar"));
    }

    @Test
    public void testAdvancingATokenMakesItCoverThePrevious() {
        KafkaTrackingToken subject = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
        }});
        KafkaTrackingToken advancedToken = subject.advancedTo(0, 1);
        assertThat(advancedToken.covers(subject)).isTrue();
        assertThat(KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(1, 0L);
        }}).covers(subject)).isFalse();
    }

    @Test
    public void testUpperBound() {
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
            put(1, 10L);
            put(2, 2L);
            put(3, 2L);
        }});

        KafkaTrackingToken second = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 10L);
            put(1, 1L);
            put(2, 2L);
            put(4, 3L);
        }});

        KafkaTrackingToken expected = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 10L);
            put(1, 10L);
            put(2, 2L);
            put(3, 2L);
            put(4, 3L);
        }});

        assertThat(first.upperBound(second)).isEqualTo(expected);
    }

    @Test
    public void testLowerBound() {
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
            put(1, 10L);
            put(2, 2L);
            put(3, 2L);
        }});

        KafkaTrackingToken second = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 10L);
            put(1, 1L);
            put(2, 2L);
            put(4, 3L);
        }});

        KafkaTrackingToken expected = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
            put(1, 1L);
            put(2, 2L);
            put(3, 0L);
            put(4, 0L);
        }});

        assertThat(first.lowerBound(second)).isEqualTo(expected);
    }

    @Test
    public void testCoversRegression() {
        KafkaTrackingToken first = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(1, 2L);
        }});

        KafkaTrackingToken second = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 1L);
        }});


        assertThat(first.covers(second)).isFalse();
    }

    @Test
    public void testRelationOfCoversAndUpperBounds() {
        java.util.Random random = new java.util.Random();
        for (int i = 0; i <= 10; i++) {
            KafkaTrackingToken first = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
                put(random.nextInt(2), (long) random.nextInt(4) + 1);
                put(random.nextInt(2), (long) random.nextInt(4) + 1);
            }});

            KafkaTrackingToken second = KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
                put(random.nextInt(2), (long) random.nextInt(4) + 1);
                put(random.nextInt(2), (long) random.nextInt(4) + 1);
            }});

            if (first.upperBound(second).equals(first)) {
                assertThat(first.covers(second))
                        .withFailMessage("The upper bound of first(%s) and second(%s) is not first, so first shouldn't cover second", first, second)
                        .isTrue();
            } else {
                assertThat(first.covers(second))
                        .withFailMessage("The upper bound of first(%s) and second(%s) is not first, so first shouldn't cover second", first, second)
                        .isFalse();
            }
        }
    }

    private static KafkaTrackingToken nonEmptyToken() {
        return KafkaTrackingToken.newInstance(new HashMap<Integer, Long>() {{
            put(0, 0L);
        }});
    }

    private static void assertKnownEventIds(KafkaTrackingToken token, Integer... expectedKnownIds) {
        assertEquals(Stream.of(expectedKnownIds).collect(toSet()),
                     new HashSet<>(token.partitionPositions().keySet()));
    }
}
