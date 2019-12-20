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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Test cases for the {@link TrackingTokenConsumerRebalanceListener} verifying the {@link Consumer#seek(TopicPartition,
 * long)} operation is called based on the given {@link KafkaTrackingToken} or defaulted to zero.
 *
 * @author Steven van Beelen
 */
class TrackingTokenConsumerRebalanceListenerTest {

    public static final String TEST_TOPIC = "some-topic";
    private final Consumer<?, ?> consumer = mock(Consumer.class);

    @Test
    void testOnPartitionsAssignedUsesTokenOffsetsUponConsumerSeek() {
        long testOffsetForPartitionZero = 5L;
        long testOffsetForPartitionOne = 10L;
        long testOffsetForPartitionTwo = 15L;

        Map<TopicPartition, Long> testPositions = new HashMap<>();
        testPositions.put(new TopicPartition(TEST_TOPIC, 0), testOffsetForPartitionZero);
        testPositions.put(new TopicPartition(TEST_TOPIC, 1), testOffsetForPartitionOne);
        testPositions.put(new TopicPartition(TEST_TOPIC, 2), testOffsetForPartitionTwo);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(testPositions);

        TopicPartition testPartitionZero = new TopicPartition(TEST_TOPIC, 0);
        TopicPartition testPartitionOne = new TopicPartition(TEST_TOPIC, 1);
        TopicPartition testPartitionTwo = new TopicPartition(TEST_TOPIC, 2);
        ArrayList<TopicPartition> testAssignedPartitions = new ArrayList<>();
        testAssignedPartitions.add(testPartitionZero);
        testAssignedPartitions.add(testPartitionOne);
        testAssignedPartitions.add(testPartitionTwo);

        TrackingTokenConsumerRebalanceListener<?, ?> testSubject =
                new TrackingTokenConsumerRebalanceListener<>(consumer, () -> testToken);

        testSubject.onPartitionsAssigned(testAssignedPartitions);

        // Offset is incremented by one, to proceed with the following record instead of the last one
        verify(consumer).seek(testPartitionZero, testOffsetForPartitionZero + 1);
        verify(consumer).seek(testPartitionOne, testOffsetForPartitionOne + 1);
        verify(consumer).seek(testPartitionTwo, testOffsetForPartitionTwo + 1);
    }

    @Test
    void testOnPartitionsAssignedUsesOffsetsOfZeroForEmptyTokenUponConsumerSeek() {
        KafkaTrackingToken testToken = KafkaTrackingToken.emptyToken();

        TopicPartition testPartitionZero = new TopicPartition(TEST_TOPIC, 0);
        TopicPartition testPartitionOne = new TopicPartition(TEST_TOPIC, 1);
        TopicPartition testPartitionTwo = new TopicPartition(TEST_TOPIC, 2);
        ArrayList<TopicPartition> testAssignedPartitions = new ArrayList<>();
        testAssignedPartitions.add(testPartitionZero);
        testAssignedPartitions.add(testPartitionOne);
        testAssignedPartitions.add(testPartitionTwo);

        TrackingTokenConsumerRebalanceListener<?, ?> testSubject =
                new TrackingTokenConsumerRebalanceListener<>(consumer, () -> testToken);

        testSubject.onPartitionsAssigned(testAssignedPartitions);

        // Offset is incremented by one, to proceed with the following record instead of the last one
        verify(consumer).seek(testPartitionZero, 0);
        verify(consumer).seek(testPartitionOne, 0);
        verify(consumer).seek(testPartitionTwo, 0);
    }
}