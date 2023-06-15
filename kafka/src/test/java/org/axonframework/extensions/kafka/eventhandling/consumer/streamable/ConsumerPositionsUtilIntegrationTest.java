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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicListSubscriber;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicPatternSubscriber;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriber;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.consumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/***
 * Integration tests spinning up a Kafka Broker to verify whether the {@link ConsumerPositionsUtil}
 * gets the correct positions.
 *
 * @author Gerard Klijs
 */

@TestMethodOrder(MethodOrderer.Alphanumeric.class)
class ConsumerPositionsUtilIntegrationTest extends KafkaContainerTest {

    private static final String RECORD_BODY = "foo";

    private static final Integer NR_PARTITIONS = 5;

    private ProducerFactory<String, String> producerFactory;
    private ConsumerFactory<String, String> consumerFactory;

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

    private static ProducerRecord<String, String> buildRecord(String topic, int partition) {
        return new ProducerRecord<>(topic, partition, null, null, RECORD_BODY);
    }

    void setUp(String topic) {
        // Retry a bit more, to give the topic time to cleanup
        KafkaAdminUtils.createTopics(getBootstrapServers(), 15, new String[]{topic});
        KafkaAdminUtils.createPartitions(getBootstrapServers(), NR_PARTITIONS, new String[]{topic});
        producerFactory = producerFactory(getBootstrapServers());
        consumerFactory = consumerFactory(getBootstrapServers());
    }

    void tearDown(String topic) {
        producerFactory.shutDown();
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), new String[]{topic});
    }

    private static Stream<Arguments> getTopicSubscribers() {
        return Stream.of(
                Arguments.of(
                        new TopicListSubscriber(Collections.singletonList("testPositionsUtil-TopicListSubscriber")),
                        "testPositionsUtil-TopicListSubscriber"),
                Arguments.of(
                        new TopicPatternSubscriber(Pattern.compile("testPositionsUtil-TopicPatternSubscriber")),
                        "testPositionsUtil-TopicPatternSubscriber")
        );
    }
    @ParameterizedTest
    @MethodSource("getTopicSubscribers")
    void positionsTest(TopicSubscriber subscriber, String topic) {
        setUp(topic);
        Consumer<?, ?> testConsumer = consumerFactory.createConsumer(null);
        assertTrue(ConsumerPositionsUtil.getHeadPositions(testConsumer, subscriber).isEmpty());
        assertTrue(ConsumerPositionsUtil.getPositionsBasedOnTime(testConsumer, subscriber, Instant.now()).isEmpty());

        int recordsPerPartitions = 5;
        Producer<String, String> producer = producerFactory.createProducer();
        publishRecordsOnPartitions(producer, topic, recordsPerPartitions, 5);

        Instant now = Instant.now();
        publishRecordsOnPartitions(producer, topic, recordsPerPartitions, 5);

        Map<TopicPartition, Long> headPositions = ConsumerPositionsUtil.getHeadPositions(testConsumer, subscriber);
        assertFalse(headPositions.isEmpty());
        assertEquals(5, headPositions.keySet().size());
        headPositions.values().forEach(p -> assertEquals(9, p));

        Map<TopicPartition, Long> positionsBasedOnTime =
                ConsumerPositionsUtil.getPositionsBasedOnTime(testConsumer, subscriber, now);
        assertFalse(positionsBasedOnTime.isEmpty());
        assertEquals(5, positionsBasedOnTime.keySet().size());
        positionsBasedOnTime.values().forEach(p -> assertEquals(4, p));
        tearDown(topic);

    }
}
