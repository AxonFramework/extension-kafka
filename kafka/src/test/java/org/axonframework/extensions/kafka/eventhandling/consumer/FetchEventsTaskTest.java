/*
 * Copyright (c) 2010-2018. Axon Framework
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.junit.*;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.emptyToken;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link FetchEventsTask}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public class FetchEventsTaskTest {

    private static final String SOME_TOPIC = "foo";
    private static final int NO_OF_PARTITIONS = 2;
    private static final int TOTAL_MESSAGES = 100;

    private KafkaConsumer testConsumer;
    private Duration testPollTimeout;
    private KafkaMessageConverter testConverter;
    private java.util.function.Consumer<FetchEventsTask> testCloseHandler;
    private Buffer testBuffer;
    private KafkaTrackingToken testToken;

    @Before
    public void setUp() {
        testConsumer = mock(KafkaConsumer.class);
        testPollTimeout = Duration.ofMillis(0);
        testConverter = mock(KafkaMessageConverter.class);
        testBuffer = mock(Buffer.class);
        testCloseHandler = task -> { /* no-op */ };
        testToken = KafkaTrackingToken.emptyToken();
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test(expected = IllegalArgumentException.class)
    public void testTaskConstructionWithInvalidConsumerShouldThrowException() {
        Consumer<Object, Object> invalidConsumer = null;

        new FetchEventsTask<>(invalidConsumer, testPollTimeout, testConverter, testBuffer, testCloseHandler, testToken);
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test(expected = IllegalArgumentException.class)
    public void testTaskConstructionWithInvalidBufferShouldThrowException() {
        Buffer<KafkaEventMessage> invalidBuffer = null;
        new FetchEventsTask<>(testConsumer, testPollTimeout, testConverter, invalidBuffer, testCloseHandler, testToken);
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test(expected = IllegalArgumentException.class)
    public void testTaskConstructionWithInvalidConverterShouldThrowException() {
        KafkaMessageConverter<Object, Object> invalidConverter = null;
        new FetchEventsTask<>(testConsumer, testPollTimeout, invalidConverter, testBuffer, testCloseHandler, testToken);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = AxonConfigurationException.class)
    public void testTaskConstructionWithNegativeTimeoutShouldThrowException() {
        Duration negativeTimeout = Duration.ofMillis(-1);
        new FetchEventsTask<>(testConsumer, negativeTimeout, testConverter, testBuffer, testCloseHandler, testToken);
    }

    @Test
    public void testTaskExecutionStartingThreadAndInterruptShouldNotCauseAnyException() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>(TOTAL_MESSAGES);

        FetchEventsTask<String, String> testSubject = new FetchEventsTask<>(
                consumer(), Duration.ofMillis(10000), new ConsumerRecordConverter(), buffer, null, emptyToken()
        );

        Thread taskRunner = new Thread(testSubject);
        taskRunner.start();
        taskRunner.interrupt();

        assertThat(buffer.isEmpty()).isTrue();
    }

    private static MockConsumer<String, String> consumer() {
        MockConsumer<String, String> consumer = new MockConsumer<>(EARLIEST);
        adjustOffsets(SOME_TOPIC, NO_OF_PARTITIONS, consumer);
        consumer.assign(partitions(NO_OF_PARTITIONS));
        consumer.seekToBeginning(partitions(NO_OF_PARTITIONS));
        addRecords(consumer);
        return consumer;
    }

    private static void addRecords(MockConsumer<String, String> consumer) {
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            consumer.addRecord(new ConsumerRecord<>(
                    "foo", i % NO_OF_PARTITIONS, i, i, NO_TIMESTAMP_TYPE, -1L, NULL_SIZE, NULL_SIZE, null, "foo-" + i
            ));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static Collection<TopicPartition> partitions(int noOfPartitions) {
        return IntStream.range(0, noOfPartitions)
                        .mapToObj(x -> new TopicPartition("foo", x))
                        .collect(Collectors.toList());
    }

    @SuppressWarnings("SameParameterValue")
    private static void adjustOffsets(String topic, int noOfPartitions, MockConsumer<String, ?> consumer) {
        Map<TopicPartition, Long> offsetsPerPartition = new HashMap<>();
        for (int i = 0; i < noOfPartitions; i++) {
            offsetsPerPartition.put(new TopicPartition(topic, i), 0L);
        }
        consumer.updateBeginningOffsets(offsetsPerPartition);
    }
}
