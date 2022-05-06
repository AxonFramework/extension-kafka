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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.Buffer;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaRecordMetaData;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.SortedKafkaMessageBuffer;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.TrackingRecordConverter;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncFetcher}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */

class AsyncFetcherIntegrationTest extends KafkaContainerTest {

    private static final String TEST_TOPIC = "some-topic";
    private static final int TEST_PARTITION = 0;

    private AsyncFetcher<String, String, KafkaEventMessage> testSubject;

    @BeforeAll
    public static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TEST_TOPIC);
    }

    @AfterAll
    public static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TEST_TOPIC);
    }

    private static Consumer<String, String> mockConsumer() {
        TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
        List<ConsumerRecord<String, String>> consumerRecords =
                Collections.singletonList(new ConsumerRecord<>(TEST_TOPIC, TEST_PARTITION, 0, null, "some-value"));
        ConsumerRecords<String, String> records =
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, consumerRecords));

        //noinspection unchecked
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumer.poll(any(Duration.class))).thenReturn(records);
        return consumer;
    }

    private static void assertMessagesCountPerPartition(int expectedMessages,
                                                        int partitionZero,
                                                        int partitionOne,
                                                        int partitionTwo,
                                                        int partitionThree,
                                                        int partitionFour,
                                                        SortedKafkaMessageBuffer<KafkaEventMessage> buffer
    ) throws InterruptedException {
        Map<Integer, Integer> received = new HashMap<>();
        for (int i = 0; i < expectedMessages; i++) {
            KafkaEventMessage m = buffer.take();
            received.putIfAbsent(m.partition(), 0);
            received.put(m.partition(), received.get(m.partition()) + 1);
        }
        assertEquals(4, received.get(partitionZero));
        assertEquals(8, received.get(partitionOne));
        assertNull(received.get(partitionTwo));
        assertEquals(5, received.get(partitionThree));
        assertEquals(9, received.get(partitionFour));
    }

    @BeforeEach
    void setUp() {
        testSubject = AsyncFetcher.<String, String, KafkaEventMessage>builder()
                                  .executorService(newSingleThreadExecutor()).build();
    }

    @AfterEach
    void tearDown() {
        testSubject.shutdown();
    }

    @Test
    void testBuildingWithInvalidPollTimeoutShouldThrowAxonConfigurationException() {
        AsyncFetcher.Builder<Object, Object, Object> builder = AsyncFetcher.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.pollTimeout(-5));
    }

    @Test
    void testBuildingWithInvalidExecutorServiceShouldThrowAxonConfigurationException() {
        AsyncFetcher.Builder<Object, Object, Object> builder = AsyncFetcher.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.executorService(null));
    }

    @Test
    @Timeout(value = 2500, unit = TimeUnit.MILLISECONDS)
    void testStartFetcherWithNullTokenShouldStartFromBeginning() throws InterruptedException {
        int expectedNumberOfMessages = 1;
        CountDownLatch messageCounter = new CountDownLatch(expectedNumberOfMessages);

        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer =
                new LatchedSortedKafkaMessageBuffer<>(expectedNumberOfMessages, messageCounter);

        testSubject.poll(
                mockConsumer(),
                new TrackingRecordConverter<>(new ConsumerRecordConverter(), KafkaTrackingToken.emptyToken()),
                testBuffer::putAll
        );

        messageCounter.await();

        assertEquals(expectedNumberOfMessages, testBuffer.size());
    }

    /**
     * This test extends outwards of the {@link AsyncFetcher}, by verifying the {@link FetchEventsTask} it creates will
     * also consume the records from an integrated Kafka set up. In doing so, the test case mirror closely what the
     * {@link StreamableKafkaMessageSource} implementation does when calling the AsyncFetcher, by for example creating a
     * {@link Consumer} and tying it to a group and topic.
     */
    @Test
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    void testStartFetcherWithExistingTokenShouldStartAtSpecificPositions() throws InterruptedException {
        // used topic for this test
        String topic = "testStartFetcherWith_ExistingToken_ShouldStartAtSpecificPositions";
        Integer nrPartitions = 5;
        KafkaAdminUtils.createTopics(getBootstrapServers(), topic);
        KafkaAdminUtils.createPartitions(getBootstrapServers(), nrPartitions, topic);

        int expectedNumberOfMessages = 26;
        CountDownLatch messageCounter = new CountDownLatch(expectedNumberOfMessages);

        int p0 = 0;
        int p1 = 1;
        int p2 = 2;
        int p3 = 3;
        int p4 = 4;
        ProducerFactory<String, String> producerFactory = publishRecords(topic, p0, p1, p2, p3, p4);
        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer =
                new LatchedSortedKafkaMessageBuffer<>(expectedNumberOfMessages, messageCounter);

        Map<TopicPartition, Long> testPositions = new HashMap<>();
        testPositions.put(new TopicPartition(topic, 0), 5L);
        testPositions.put(new TopicPartition(topic, 1), 1L);
        testPositions.put(new TopicPartition(topic, 2), 9L);
        testPositions.put(new TopicPartition(topic, 3), 4L);
        testPositions.put(new TopicPartition(topic, 4), 0L);
        KafkaTrackingToken testStartToken = KafkaTrackingToken.newInstance(testPositions);

        Consumer<String, String> testConsumer = consumerFactory(getBootstrapServers()).createConsumer(null);
        List<TopicPartition> all = testConsumer.listTopics().entrySet()
                                               .stream()
                                               .filter(e -> e.getKey().equals(topic))
                                               .flatMap(e -> e.getValue().stream())
                                               .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                                                                                        partitionInfo.partition()))
                                               .collect(Collectors.toList());
        testConsumer.assign(all);
        all.forEach(assignedPartition -> {
            Map<TopicPartition, Long> tokenPartitionPositions = testStartToken.getPositions();
            long offset = 0L;
            if (tokenPartitionPositions.containsKey(assignedPartition)) {
                offset = tokenPartitionPositions.get(assignedPartition) + 1;
            }
            testConsumer.seek(assignedPartition, offset);
        });

        testSubject.poll(
                testConsumer,
                new TrackingRecordConverter<>(new ConsumerRecordConverter(), testStartToken),
                testBuffer::putAll
        );

        messageCounter.await();
        assertEquals(expectedNumberOfMessages, testBuffer.size());
        assertMessagesCountPerPartition(expectedNumberOfMessages, p0, p1, p2, p3, p4, testBuffer);

        producerFactory.shutDown();
    }

    private ProducerFactory<String, String> publishRecords(String topic,
                                                           int partitionZero,
                                                           int partitionOne,
                                                           int partitionTwo,
                                                           int partitionThree,
                                                           int partitionFour) {
        ProducerFactory<String, String> pf = producerFactory(getBootstrapServers());
        Producer<String, String> producer = pf.createProducer();
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, partitionZero, null, null, "foo-" + partitionZero + "-" + i));
            producer.send(new ProducerRecord<>(topic, partitionOne, null, null, "foo-" + partitionOne + "-" + i));
            producer.send(new ProducerRecord<>(topic, partitionTwo, null, null, "foo-" + partitionTwo + "-" + i));
            producer.send(new ProducerRecord<>(topic, partitionThree, null, null, "foo-" + partitionThree + "-" + i));
            producer.send(new ProducerRecord<>(topic, partitionFour, null, null, "foo-" + partitionFour + "-" + i));
        }
        producer.flush();
        return pf;
    }

    /**
     * A {@link SortedKafkaMessageBuffer} extension intended to count down until the expected number of elements have
     * been added.
     *
     * @param <E> the type of the elements stored in this {@link Buffer} implementation
     */
    private static class LatchedSortedKafkaMessageBuffer<E extends Comparable<?> & KafkaRecordMetaData<?>>
            extends SortedKafkaMessageBuffer<E> {

        private final CountDownLatch bufferedMessageLatch;

        private LatchedSortedKafkaMessageBuffer(int capacity, CountDownLatch bufferedMessageLatch) {
            super(capacity);
            this.bufferedMessageLatch = bufferedMessageLatch;
        }

        @Override
        public void put(E comparable) throws InterruptedException {
            super.put(comparable);
            bufferedMessageLatch.countDown();
        }

        @Override
        public void putAll(Collection<E> c) throws InterruptedException {
            super.putAll(c);
            c.forEach(element -> bufferedMessageLatch.countDown());
        }
    }
}
