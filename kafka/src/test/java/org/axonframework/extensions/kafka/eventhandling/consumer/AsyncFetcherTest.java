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
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher.builder;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncFetcher}.
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

    private volatile KafkaTrackingToken currentToken;

    @SuppressWarnings("unchecked")
    private static ConsumerFactory<String, String> mockConsumerFactory(String topic) {
        ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
        Consumer<String, String> consumer = mock(Consumer.class);
        when(consumerFactory.createConsumer()).thenReturn(consumer);

        int partition = 0;
        Map<TopicPartition, List<ConsumerRecord<String, String>>> record = new HashMap<>();
        record.put(new TopicPartition(topic, partition), Collections.singletonList(new ConsumerRecord<>(
                topic, partition, 0, null, "hello"
        )));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(record);
        when(consumer.poll(any(Duration.class))).thenReturn(records);

        return consumerFactory;
    }

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

    @Test
    public void testStartFetcherWithNullTokenShouldStartFromBeginning() throws InterruptedException {
        KafkaTrackingToken expectedToken = KafkaTrackingToken.newInstance(Collections.singletonMap(0, 0L));
        CountDownLatch messageCounter = new CountDownLatch(1);

        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer = new SortedKafkaMessageBuffer<>(1);
        String testTopic = "foo";
        Fetcher testSubject = AsyncFetcher.<String, String>builder()
                .consumerFactory(mockConsumerFactory(testTopic))
                .bufferFactory(() -> testBuffer)
                .executorService(newSingleThreadExecutor())
                .messageConverter(new ValueConverter())
                .topic(testTopic)
                .consumerRecordCallback(countMessage(messageCounter))
                .pollTimeout(3000)
                .build();

        testSubject.start(null);

        messageCounter.await();

        assertThat(testBuffer.size()).isOne();
        assertThat(currentToken).isEqualTo(expectedToken);

        testSubject.shutdown();
    }

    @Test
    public void testStartFetcherWithExistingTokenShouldStartAtSpecificPositions() throws InterruptedException {
        int expectedMessages = 26;
        Map<Integer, Long> expectedPartitionPositions = new HashMap<>();
        expectedPartitionPositions.put(0, 9L);
        expectedPartitionPositions.put(1, 9L);
        expectedPartitionPositions.put(2, 9L);
        expectedPartitionPositions.put(3, 9L);
        expectedPartitionPositions.put(4, 9L);
        KafkaTrackingToken expectedToken = KafkaTrackingToken.newInstance(expectedPartitionPositions);
        CountDownLatch messageCounter = new CountDownLatch(expectedMessages);

        String testTopic = "testStartFetcherWith_ExistingToken_ShouldStartAtSpecificPositions";
        int p0 = 0;
        int p1 = 1;
        int p2 = 2;
        int p3 = 3;
        int p4 = 4;
        ProducerFactory<String, String> producerFactory = publishRecords(testTopic, p0, p1, p2, p3, p4);
        SortedKafkaMessageBuffer<KafkaEventMessage> testBuffer = new SortedKafkaMessageBuffer<>(expectedMessages);
        Fetcher testSubject = AsyncFetcher.<String, String>builder()
                .consumerFactory(consumerFactory(kafkaBroker, testTopic))
                .bufferFactory(() -> testBuffer)
                .messageConverter(new ValueConverter())
                .topic(testTopic)
                .consumerRecordCallback(countMessage(messageCounter))
                .pollTimeout(3000)
                .build();

        Map<Integer, Long> testPartitionPositions = new HashMap<>();
        testPartitionPositions.put(0, 5L);
        testPartitionPositions.put(1, 1L);
        testPartitionPositions.put(2, 9L);
        testPartitionPositions.put(3, 4L);
        testPartitionPositions.put(4, 0L);
        KafkaTrackingToken testStartToken = KafkaTrackingToken.newInstance(testPartitionPositions);
        testSubject.start(testStartToken);

        messageCounter.await();

        assertThat(testBuffer.size()).isEqualTo(expectedMessages);
        assertThat(currentToken).isEqualTo(expectedToken);
        assertMessagesCountPerPartition(expectedMessages, p0, p1, p2, p3, p4, testBuffer);

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

    private BiFunction<ConsumerRecord<String, String>, KafkaTrackingToken, Void> countMessage(CountDownLatch counter) {
        return (r, t) -> {
            currentToken = t;
            counter.countDown();
            return null;
        };
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

    static class ValueConverter implements KafkaMessageConverter<String, String> {

        @Override
        public ProducerRecord<String, String> createKafkaMessage(EventMessage<?> eventMessage, String topic) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public Optional<EventMessage<?>> readKafkaMessage(ConsumerRecord<String, String> consumerRecord) {
            return Optional.of(asEventMessage(consumerRecord.value()));
        }
    }
}
