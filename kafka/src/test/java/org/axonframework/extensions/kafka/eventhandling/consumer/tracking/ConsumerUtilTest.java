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

package org.axonframework.extensions.kafka.eventhandling.consumer.tracking;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Function1;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static kafka.utils.TestUtils.pollUntilAtLeastNumRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.consumer.tracking.ConsumerUtil.seek;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;

/***
 * Tests for the {@link ConsumerUtil} class asserting utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        topics = {
                "testSeekUsing_NullToken_ConsumerStartsAtPositionZero",
                "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero",
                "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition",
                "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords"
        },
        partitions = 5
)
public class ConsumerUtilTest {

    private static final String RECORD_BODY = "foo";
    private static final Function1<ConsumerRecord<byte[], byte[]>, Object> COUNT_ALL = record -> true;

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;
    private ProducerFactory<String, String> producerFactory;
    private ConsumerFactory<String, String> consumerFactory;

    @Before
    public void setUp() {
        producerFactory = producerFactory(kafkaBroker);
        consumerFactory = consumerFactory(kafkaBroker);
    }

    @After
    public void tearDown() {
        producerFactory.shutDown();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSeekUsingNullTokenConsumerStartsAtPositionZero() {
        String topic = "testSeekUsing_NullToken_ConsumerStartsAtPositionZero";
        int numberOfPartitions = kafkaBroker.getPartitionsPerTopic();
        int recordsPerPartitions = 1;
        publishRecordsOnPartitions(producerFactory.createProducer(), topic, recordsPerPartitions, numberOfPartitions);

        int expectedRecordCount = numberOfPartitions * recordsPerPartitions;
        AtomicInteger recordCounter = new AtomicInteger();
        KafkaTrackingToken testToken = null;

        Consumer<?, ?> testSubject = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        seek(topic, testSubject, testToken);

        getRecords(testSubject).forEach(record -> {
            assertThat(record.offset()).isZero();
            recordCounter.getAndIncrement();
        });
        assertThat(recordCounter.get()).isEqualTo(expectedRecordCount);

        testSubject.close();
    }

    @Test
    public void testSeekUsingEmptyTokenConsumerStartsAtPositionZero() {
        String topic = "testSeekUsing_EmptyToken_ConsumerStartsAtPositionZero";
        int numberOfPartitions = kafkaBroker.getPartitionsPerTopic();
        int recordsPerPartitions = 1;
        publishRecordsOnPartitions(producerFactory.createProducer(), topic, recordsPerPartitions, numberOfPartitions);

        int expectedRecordCount = numberOfPartitions * recordsPerPartitions;
        AtomicInteger recordCounter = new AtomicInteger();
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(emptyMap());

        Consumer<?, ?> testSubject = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        seek(topic, testSubject, testToken);

        getRecords(testSubject).forEach(record -> {
            assertThat(record.offset()).isZero();
            recordCounter.getAndIncrement();
        });
        assertThat(recordCounter.get()).isEqualTo(expectedRecordCount);

        testSubject.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSeekUsingExistingTokenConsumerStartsAtSpecificPosition() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition";
        int recordsPerPartitions = 10;
        publishRecordsOnPartitions(
                producerFactory.createProducer(), topic, recordsPerPartitions, kafkaBroker.getPartitionsPerTopic()
        );

        Map<Integer, Long> partitionPositions = new HashMap<>();
        partitionPositions.put(0, 5L);
        partitionPositions.put(1, 1L);
        partitionPositions.put(2, 9L);
        partitionPositions.put(3, 4L);
        partitionPositions.put(4, 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(partitionPositions);
        // This number corresponds to the steps the five partition's
        //  their offsets will increase given the published number of `recordsPerPartitions`
        int numberOfRecordsToConsume = 26;

        Consumer<?, ?> testSubject = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        seek(topic, testSubject, testToken);

        Seq<ConsumerRecord<byte[], byte[]>> resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testSubject, numberOfRecordsToConsume);
        resultRecords.foreach(resultRecord -> assertThat(resultRecord.offset())
                .isGreaterThan(partitionPositions.get(resultRecord.partition())));
        assertThat(resultRecords.count(COUNT_ALL)).isEqualTo(numberOfRecordsToConsume);

        testSubject.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSeekUsingExistingTokenConsumerStartsAtSpecificPositionAndCanContinueReadingNewRecords() {
        String topic = "testSeekUsing_ExistingToken_ConsumerStartsAtSpecificPosition_AndCanContinueReadingNewRecords";
        int recordsPerPartitions = 10;
        Producer<String, String> testProducer = producerFactory.createProducer();
        publishRecordsOnPartitions(
                testProducer, topic, recordsPerPartitions, kafkaBroker.getPartitionsPerTopic()
        );

        Map<Integer, Long> partitionPositions = new HashMap<>();
        partitionPositions.put(0, 5L);
        partitionPositions.put(1, 1L);
        partitionPositions.put(2, 9L);
        partitionPositions.put(3, 4L);
        partitionPositions.put(4, 0L);
        KafkaTrackingToken testToken = KafkaTrackingToken.newInstance(partitionPositions);
        // This number corresponds to the steps the five partition's
        //  their offsets will increase given the published number of `recordsPerPartitions`
        int numberOfRecordsToConsume = 26;

        Consumer<?, ?> testSubject = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        seek(topic, testSubject, testToken);

        Seq<ConsumerRecord<byte[], byte[]>> resultRecords =
                pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testSubject, numberOfRecordsToConsume);
        resultRecords.foreach(resultRecord -> assertThat(resultRecord.offset())
                .isGreaterThan(partitionPositions.get(resultRecord.partition())));
        assertThat(resultRecords.count(COUNT_ALL)).isEqualTo(numberOfRecordsToConsume);

        publishNewRecords(testProducer, topic);
        int secondNumberOfRecords = 4; // The `publishNewRecords(Producer, String)` produces 4 new records
        resultRecords = pollUntilAtLeastNumRecords((KafkaConsumer<byte[], byte[]>) testSubject, secondNumberOfRecords);

        resultRecords.foreach(resultRecord -> assertThat(resultRecord.offset()).isEqualTo(10));
        assertThat(resultRecords.count(COUNT_ALL)).isEqualTo(secondNumberOfRecords);

        testSubject.close();
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