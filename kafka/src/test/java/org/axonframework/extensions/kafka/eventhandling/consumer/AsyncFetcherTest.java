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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher.builder;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncFetcher}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(topics = {"testStartFetcherWith_ExistingToken_ShouldStartAtSpecificPositions"}, partitions = 5)
public class AsyncFetcherTest {

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @Test(expected = AxonConfigurationException.class)
    public void testBuilderCreationInvalidTopic() {
        builder().consumerFactory(new HashMap<>()).topic(null).build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuilderCreationInvalidConsumerFactory() {
        builder().consumerFactory((ConsumerFactory<Object, Object>) null);
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuilderCreationInvalidConverter() {
        builder().consumerFactory(new HashMap<>()).messageConverter(null);
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuilderCreationInvalidBuffer() {
        builder().consumerFactory(new HashMap<>()).bufferFactory(null);
    }

    @Test(timeout = 2500)
    public void testStartFetcherWithNullTokenShouldStartFromBeginning() throws InterruptedException {
        int expectedNumberOfMessages = 1;
        CountDownLatch messageCounter = new CountDownLatch(expectedNumberOfMessages);

        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer =
                new LatchedSortedKafkaMessageBuffer<>(expectedNumberOfMessages, messageCounter);

        String testTopic = "foo";
        Fetcher testSubject = AsyncFetcher.<String, String>builder()
                .consumerFactory(mockConsumerFactory(testTopic))
                .bufferFactory(() -> testBuffer)
                .executorService(newSingleThreadExecutor())
                .messageConverter(new ConsumerRecordConverter())
                .topic(testTopic)
                .build();

        testSubject.start(null, DEFAULT_GROUP_ID);

        messageCounter.await();

        assertThat(testBuffer.size()).isEqualTo(expectedNumberOfMessages);

        testSubject.shutdown();
    }

    @SuppressWarnings("unchecked")
    private static ConsumerFactory<String, String> mockConsumerFactory(String topic) {
        ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumerFactory.createConsumer(DEFAULT_GROUP_ID)).thenReturn(consumer);

        int partition = 0;
        Map<TopicPartition, List<ConsumerRecord<String, String>>> record = new HashMap<>();
        record.put(new TopicPartition(topic, partition), Collections.singletonList(new ConsumerRecord<>(
                topic, partition, 0, null, "hello"
        )));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(record);
        when(consumer.poll(any(Duration.class))).thenReturn(records);

        return consumerFactory;
    }

    @Test(timeout = 5000)
    public void testStartFetcherWithExistingTokenShouldStartAtSpecificPositions() throws InterruptedException {
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
        Fetcher testSubject = AsyncFetcher.<String, String>builder()
                .consumerFactory(consumerFactory(kafkaBroker))
                .bufferFactory(() -> testBuffer)
                .messageConverter(new ConsumerRecordConverter())
                .topic(testTopic)
                .build();

        Map<Integer, Long> testPartitionPositions = new HashMap<>();
        testPartitionPositions.put(0, 5L);
        testPartitionPositions.put(1, 1L);
        testPartitionPositions.put(2, 9L);
        testPartitionPositions.put(3, 4L);
        testPartitionPositions.put(4, 0L);
        KafkaTrackingToken testStartToken = KafkaTrackingToken.newInstance(testPartitionPositions);
        testSubject.start(testStartToken, DEFAULT_GROUP_ID);

        messageCounter.await();
        assertThat(testBuffer.size()).isEqualTo(expectedNumberOfMessages);
        assertMessagesCountPerPartition(expectedNumberOfMessages, p0, p1, p2, p3, p4, testBuffer);

        producerFactory.shutDown();
        testSubject.shutdown();
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
    private static class LatchedSortedKafkaMessageBuffer<E extends Comparable & KafkaRecordMetaData>
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
