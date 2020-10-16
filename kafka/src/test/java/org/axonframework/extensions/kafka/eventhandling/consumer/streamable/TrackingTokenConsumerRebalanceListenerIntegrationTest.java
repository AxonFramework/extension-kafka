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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import scala.Function1;
import scala.collection.Seq;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static kafka.utils.TestUtils.pollUntilAtLeastNumRecords;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;

/***
 * Integration tests spinning up a Kafka Broker to verify whether the {@link TrackingTokenConsumerRebalanceListener}
 * starts a seek operation on the expected offsets from a {@link Consumer#poll(Duration)} perspective.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(
        topics = {
                "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero",
                "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition",
                "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords"
        },
        partitions = 5
)
class TrackingTokenConsumerRebalanceListenerIntegrationTest {

    private static final String RECORD_BODY = "foo";
    private static final Function1<ConsumerRecord<byte[], byte[]>, Object> COUNT_ALL = record -> true;

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;
    private ProducerFactory<String, String> producerFactory;
    private ConsumerFactory<String, String> consumerFactory;

    @BeforeEach
    void setUp() {
        producerFactory = producerFactory(kafkaBroker);
        consumerFactory = consumerFactory(kafkaBroker);
    }

    @AfterEach
    void tearDown() {
        producerFactory.shutDown();
    }

    @Test
    void testSeekUsingEmptyTokenConsumerStartsAtPositionZero() {
        String topic = "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero";
        int numberOfPartitions = kafkaBroker.getPartitionsPerTopic();
        int recordsPerPartitions = 1;
        publishRecordsOnPartitions(producerFactory.createProducer(), topic, recordsPerPartitions, numberOfPartitions);

        int expectedRecordCount = numberOfPartitions * recordsPerPartitions;
        AtomicInteger recordCounter = new AtomicInteger();
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(emptyMap());

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(
                Collections.singletonList(topic),
                new TrackingTokenConsumerRebalanceListener<>(testConsumer, () -> testToken)
        );

        getRecords(testConsumer).forEach(record -> {
            assertEquals(0, record.offset());
            recordCounter.getAndIncrement();
        });
        assertEquals(expectedRecordCount, recordCounter.get());

        testConsumer.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSeekUsingExistingTokenConsumerStartsAtSpecificPosition() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition";
        int recordsPerPartitions = 10;
        publishRecordsOnPartitions(
                producerFactory.createProducer(), topic, recordsPerPartitions, kafkaBroker.getPartitionsPerTopic()
        );

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

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(
                Collections.singletonList(topic),
                new TrackingTokenConsumerRebalanceListener<>(testConsumer, () -> testToken)
        );

        Seq<ConsumerRecord<byte[], byte[]>> resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testConsumer, numberOfRecordsToConsume, 500);
        resultRecords.foreach(resultRecord -> {
            TopicPartition resultTopicPartition = new TopicPartition(resultRecord.topic(), resultRecord.partition());
            assertTrue(resultRecord.offset() > positions.get(resultTopicPartition));
            // This ugly stuff is needed since I have to deal with a scala.collection.Seq
            return null;
        });
        assertEquals(numberOfRecordsToConsume, resultRecords.count(COUNT_ALL));

        testConsumer.close();
    }

    @Test
    void testSeekUsingExistingTokenConsumerStartsAtSpecificPositionAndCanContinueReadingNewRecords() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords";
        int recordsPerPartitions = 10;
        Producer<String, String> testProducer = producerFactory.createProducer();
        publishRecordsOnPartitions(
                testProducer, topic, recordsPerPartitions, kafkaBroker.getPartitionsPerTopic()
        );

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

        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(
                Collections.singletonList(topic),
                new TrackingTokenConsumerRebalanceListener<>(testConsumer, () -> testToken)
        );

        //noinspection unchecked
        Seq<ConsumerRecord<byte[], byte[]>> resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testConsumer, numberOfRecordsToConsume, 500);
        resultRecords.foreach(resultRecord -> {
            TopicPartition resultTopicPartition = new TopicPartition(resultRecord.topic(), resultRecord.partition());
            assertTrue(resultRecord.offset() > positions.get(resultTopicPartition));
            // This ugly stuff is needed since I have to deal with a scala.collection.Seq
            return null;
        });
        assertEquals(numberOfRecordsToConsume, resultRecords.count(COUNT_ALL));

        publishNewRecords(testProducer, topic);
        int secondNumberOfRecords = 4; // The `publishNewRecords(Producer, String)` produces 4 new records
        //noinspection unchecked
        resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testConsumer, secondNumberOfRecords, 500);

        resultRecords.foreach(resultRecord -> {
            assertEquals(10, resultRecord.offset());
            // This ugly stuff is needed since I have to deal with a scala.collection.Seq
            return null;
        });
        assertEquals(secondNumberOfRecords, resultRecords.count(COUNT_ALL));

        testConsumer.close();
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
}