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
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.TrackingTokenConsumerRebalanceListener;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
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
@DirtiesContext
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(topics = {"testStartFetcherWith_ExistingToken_ShouldStartAtSpecificPositions"}, partitions = 5)
class AsyncFetcherTest {

    private static final String TEST_TOPIC = "some-topic";
    private static final int TEST_PARTITION = 0;

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    private AsyncFetcher<String, String, KafkaEventMessage> testSubject;

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
        assertThrows(AxonConfigurationException.class, () -> AsyncFetcher.builder().pollTimeout(-5));
    }

    @Test
    void testBuildingWithInvalidExecutorServiceShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> AsyncFetcher.builder().executorService(null));
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

        assertThat(testBuffer.size()).isEqualTo(expectedNumberOfMessages);
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

    /**
     * This test extends outwards of the {@link AsyncFetcher}, by verifying the {@link FetchEventsTask} it creates will
     * also consume the records from an integrated Kafka set up. In doing so, the test case mirror closely what the
     * {@link StreamableKafkaMessageSource} implementation does when calling the AsyncFetcher, by for example creating a
     * {@link Consumer} and tying it to a group and topic.
     */
    @Test
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    void testStartFetcherWithExistingTokenShouldStartAtSpecificPositions() throws InterruptedException {
        int expectedNumberOfMessages = 26;
        CountDownLatch messageCounter = new CountDownLatch(expectedNumberOfMessages);

        String testTopic = "testStartFetcherWith_ExistingToken_ShouldStartAtSpecificPositions";
        int p0 = 0;
        int p1 = 1;
        int p2 = 2;
        int p3 = 3;
        int p4 = 4;
        ProducerFactory<String, String> producerFactory = publishRecords(testTopic, p0, p1, p2, p3, p4);
        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer =
                new LatchedSortedKafkaMessageBuffer<>(expectedNumberOfMessages, messageCounter);

        Map<TopicPartition, Long> testPositions = new HashMap<>();
        testPositions.put(new TopicPartition(testTopic, 0), 5L);
        testPositions.put(new TopicPartition(testTopic, 1), 1L);
        testPositions.put(new TopicPartition(testTopic, 2), 9L);
        testPositions.put(new TopicPartition(testTopic, 3), 4L);
        testPositions.put(new TopicPartition(testTopic, 4), 0L);
        KafkaTrackingToken testStartToken = KafkaTrackingToken.newInstance(testPositions);

        Consumer<String, String> testConsumer = consumerFactory(kafkaBroker).createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(
                Collections.singletonList(testTopic),
                new TrackingTokenConsumerRebalanceListener<>(testConsumer, () -> testStartToken)
        );

        testSubject.poll(
                testConsumer,
                new TrackingRecordConverter<>(new ConsumerRecordConverter(), testStartToken),
                testBuffer::putAll
        );

        messageCounter.await();
        assertThat(testBuffer.size()).isEqualTo(expectedNumberOfMessages);
        assertMessagesCountPerPartition(expectedNumberOfMessages, p0, p1, p2, p3, p4, testBuffer);

        producerFactory.shutDown();
    }

    private ProducerFactory<String, String> publishRecords(String topic,
                                                           int partitionZero,
                                                           int partitionOne,
                                                           int partitionTwo,
                                                           int partitionThree,
                                                           int partitionFour) {
        ProducerFactory<String, String> pf = producerFactory(kafkaBroker);
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
        assertThat(received.get(partitionZero)).isEqualTo(4);
        assertThat(received.get(partitionOne)).isEqualTo(8);
        assertThat(received.get(partitionTwo)).isNull();
        assertThat(received.get(partitionThree)).isEqualTo(5);
        assertThat(received.get(partitionFour)).isEqualTo(9);
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
