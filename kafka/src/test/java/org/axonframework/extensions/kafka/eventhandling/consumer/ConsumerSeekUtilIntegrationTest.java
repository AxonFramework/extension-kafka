/*
 * Copyright (c) 2010-2023. Axon Framework
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.KafkaTestUtils.getRecords;
import static org.axonframework.extensions.kafka.eventhandling.util.KafkaTestUtils.pollUntilAtLeastNumRecords;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.junit.jupiter.api.Assertions.*;

/***
 * Integration tests spinning up a Kafka Broker to verify whether the {@link ConsumerSeekUtil}
 * starts a seek operation on the expected offsets from a {@link Consumer#poll(Duration)} perspective.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @author Gerard Klijs
 */

class ConsumerSeekUtilIntegrationTest extends KafkaContainerTest {

    private static final String RECORD_BODY = "foo";

    private static final String[] TOPICS = {
            "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero",
            "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition",
            "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords"};
    private static final Integer NR_PARTITIONS = 5;

    private ProducerFactory<String, String> producerFactory;
    private ConsumerFactory<String, String> consumerFactory;

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TOPICS);
        KafkaAdminUtils.createPartitions(getBootstrapServers(), NR_PARTITIONS, TOPICS);
    }

    @AfterAll
    public static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPICS);
    }

    private static void publishRecordsOnPartitions(Producer<String, String> producer,
                                                   String topic,
                                                   int recordsPerPartitions,
                                                   int partitionsPerTopic) {
        for (int i = 0; i < recordsPerPartitions; i++) {
            for (int p = 0; p < partitionsPerTopic; p++) {
                producer.send(buildRecord(topic, p));
            }
        }
        producer.flush();
    }

    private static void publishNewRecords(Producer<String, String> producer, String topic) {
        producer.send(buildRecord(topic, 0));
        producer.send(buildRecord(topic, 1));
        producer.send(buildRecord(topic, 2));
        producer.send(buildRecord(topic, 3));
        producer.flush();
    }

    private static ProducerRecord<String, String> buildRecord(String topic, int partition) {
        return new ProducerRecord<>(topic, partition, null, null, RECORD_BODY);
    }

    @BeforeEach
    void setUp() {
        producerFactory = producerFactory(getBootstrapServers());
        consumerFactory = consumerFactory(getBootstrapServers());
    }

    @AfterEach
    void tearDown() {
        producerFactory.shutDown();
    }

    @Test
    void seekUsingEmptyTokenConsumerStartsAtPositionZero() {
        String topic = "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero";
        int recordsPerPartitions = 1;
        Producer<String, String> producer = producerFactory.createProducer();
        int numberOfPartitions = producer.partitionsFor(topic).size();
        publishRecordsOnPartitions(producer, topic, recordsPerPartitions, numberOfPartitions);

        int expectedRecordCount = numberOfPartitions * recordsPerPartitions;
        AtomicInteger recordCounter = new AtomicInteger();
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(emptyMap());

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(null);
        ConsumerSeekUtil.seekToCurrentPositions(testConsumer, () -> testToken, Collections.singletonList(topic));

        getRecords(testConsumer).forEach(record -> {
            assertEquals(0, record.offset());
            recordCounter.getAndIncrement();
        });
        assertEquals(expectedRecordCount, recordCounter.get());

        testConsumer.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void seekUsingExistingTokenConsumerStartsAtSpecificPosition() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition";
        int recordsPerPartitions = 10;
        Producer<String, String> producer = producerFactory.createProducer();
        int numberOfPartitions = producer.partitionsFor(topic).size();
        publishRecordsOnPartitions(producer, topic, recordsPerPartitions, numberOfPartitions);

        Map<TopicPartition, Long> positions = new HashMap<>();
        positions.put(new TopicPartition(topic, 0), 5L);
        positions.put(new TopicPartition(topic, 1), 1L);
        positions.put(new TopicPartition(topic, 2), 9L);
        positions.put(new TopicPartition(topic, 3), 4L);
        positions.put(new TopicPartition(topic, 4), 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(positions);
        // This number corresponds to the steps the five partition's
        //  their offsets will increase given the published number of `recordsPerPartitions`
        int numberOfRecordsToConsume = 26;

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(null);
        ConsumerSeekUtil.seekToCurrentPositions(testConsumer, () -> testToken, Collections.singletonList(topic));

        List<ConsumerRecord<byte[], byte[]>> resultRecords = pollUntilAtLeastNumRecords(
                (KafkaConsumer<byte[], byte[]>) testConsumer, numberOfRecordsToConsume, 2500
        );
        resultRecords.forEach(resultRecord -> {
            TopicPartition resultTopicPartition = new TopicPartition(resultRecord.topic(), resultRecord.partition());
            assertTrue(resultRecord.offset() > positions.get(resultTopicPartition));
        });
        assertEquals(numberOfRecordsToConsume, resultRecords.size());

        testConsumer.close();
    }

    @Test
    void seekUsingExistingTokenConsumerStartsAtSpecificPositionAndCanContinueReadingNewRecords() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords";
        int recordsPerPartitions = 10;
        Producer<String, String> testProducer = producerFactory.createProducer();
        int numberOfPartitions = testProducer.partitionsFor(topic).size();

        publishRecordsOnPartitions(testProducer, topic, recordsPerPartitions, numberOfPartitions);

        Map<TopicPartition, Long> positions = new HashMap<>();
        positions.put(new TopicPartition(topic, 0), 5L);
        positions.put(new TopicPartition(topic, 1), 1L);
        positions.put(new TopicPartition(topic, 2), 9L);
        positions.put(new TopicPartition(topic, 3), 4L);
        positions.put(new TopicPartition(topic, 4), 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(positions);
        // This number corresponds to the steps the five partition's
        //  their offsets will increase given the published number of `recordsPerPartitions`
        int numberOfRecordsToConsume = 26;

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(null);
        ConsumerSeekUtil.seekToCurrentPositions(testConsumer, () -> testToken, Collections.singletonList(topic));

        //noinspection unchecked
        List<ConsumerRecord<byte[], byte[]>> resultRecords = pollUntilAtLeastNumRecords(
                (KafkaConsumer<byte[], byte[]>) testConsumer, numberOfRecordsToConsume, 2500
        );
        resultRecords.forEach(resultRecord -> {
            TopicPartition resultTopicPartition = new TopicPartition(resultRecord.topic(), resultRecord.partition());
            assertTrue(resultRecord.offset() > positions.get(resultTopicPartition));
        });
        assertEquals(numberOfRecordsToConsume, resultRecords.size());

        publishNewRecords(testProducer, topic);
        int secondNumberOfRecords = 4; // The `publishNewRecords(Producer, String)` produces 4 new records
        //noinspection unchecked
        resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testConsumer, secondNumberOfRecords, 500);

        resultRecords.forEach(resultRecord -> assertEquals(10, resultRecord.offset()));
        assertEquals(secondNumberOfRecords, resultRecords.size());

        testConsumer.close();
    }
}
