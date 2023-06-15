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

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test cases for the {@link ConsumerSeekUtil} verifying the {@link Consumer#seek(TopicPartition, long)} operation is
 * called based on the given {@link KafkaTrackingToken} or defaulted to zero.
 *
 * @author Steven van Beelen
 * @author Gerard Klijs
 */
class ConsumerSeekUtilTest {

    public static final String TEST_TOPIC = "some-topic";
    private final Consumer<?, ?> consumer = mock(Consumer.class);

    private static Stream<TopicSubscriber> getTopicSubscribers() {
        return Stream.of(
                new TopicListSubscriber(Collections.singletonList(TEST_TOPIC)),
                new TopicPatternSubscriber(Pattern.compile(TEST_TOPIC))
        );
    }

    @ParameterizedTest
    @MethodSource("getTopicSubscribers")
    void testOnPartitionsAssignedUsesTokenOffsetsUponConsumerSeek(TopicSubscriber subscriber) {
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

        Map<TopicPartition, Long> testAssignedPartitions = new HashMap<>();
        testAssignedPartitions.put(testPartitionZero, testOffsetForPartitionZero);
        testAssignedPartitions.put(testPartitionOne, testOffsetForPartitionOne);
        testAssignedPartitions.put(testPartitionTwo, testOffsetForPartitionTwo);
        ArrayList<TopicPartition> testAssignedPartitionList = new ArrayList<>(testAssignedPartitions.keySet());

        doReturn(listTopics(testAssignedPartitionList)).when(consumer).listTopics();

        ConsumerSeekUtil.seekToCurrentPositions(consumer, () -> testToken, subscriber);
        for (Map.Entry<TopicPartition, Long> entry : testAssignedPartitions.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long offset = entry.getValue();
            verify(consumer).seek(topicPartition, offset + 1);
        }

    }

    @ParameterizedTest
    @MethodSource("getTopicSubscribers")
    void testOnPartitionsAssignedUsesOffsetsOfZeroForEmptyTokenUponConsumerSeek(TopicSubscriber subscriber) {
        KafkaTrackingToken testToken = KafkaTrackingToken.emptyToken();

        TopicPartition testPartitionZero = new TopicPartition(TEST_TOPIC, 0);
        TopicPartition testPartitionOne = new TopicPartition(TEST_TOPIC, 1);
        TopicPartition testPartitionTwo = new TopicPartition(TEST_TOPIC, 2);
        List<TopicPartition> testAssignedPartitions = new ArrayList<>();
        testAssignedPartitions.add(testPartitionZero);
        testAssignedPartitions.add(testPartitionOne);
        testAssignedPartitions.add(testPartitionTwo);
        doReturn(listTopics(testAssignedPartitions)).when(consumer).listTopics();

        ConsumerSeekUtil.seekToCurrentPositions(consumer, () -> testToken, subscriber);

        verify(consumer).seek(testPartitionZero, 0);
        verify(consumer).seek(testPartitionOne, 0);
        verify(consumer).seek(testPartitionTwo, 0);
    }

    private Map<String, List<PartitionInfo>> listTopics(List<TopicPartition> partitions) {
        Map<String, List<PartitionInfo>> topics = new HashMap<>();
        partitions.forEach(p -> topics.compute(p.topic(), (k, o) -> {
            if (o == null) {
                return Collections.singletonList(toPartitionInfo(p));
            } else {
                List<PartitionInfo> newList = new ArrayList<>(o);
                newList.add(toPartitionInfo(p));
                return newList;
            }
        }));
        return topics;
    }

    private PartitionInfo toPartitionInfo(TopicPartition topicPartition) {
        return new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, null, null);
    }
}