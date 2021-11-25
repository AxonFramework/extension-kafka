/*
 * Copyright (c) 2010-2021. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerClusterTest;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory.builder;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.minimalTransactional;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.transactionalProducerFactory;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link DefaultProducerFactory} when running a cluster.
 *
 * @author Steven van Beelen
 * @author Lucas Campos
 * @author Nakul Mishra
 */
class DefaultProducerFactoryClusteringIntegrationTest extends KafkaContainerClusterTest {

    private static final String TOPIC = "testSendingMessagesUsingMultipleTransactionalProducers";

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TOPIC);
    }

    @AfterAll
    static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPIC);
    }

    @SuppressWarnings("SameParameterValue")
    private static Future<RecordMetadata> send(Producer<String, String> producer, String topic, String message) {
        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, message));
        producer.flush();
        return result;
    }

    private static void cleanup(ProducerFactory<String, String> pf, List<Producer<String, String>> producers) {
        producers.forEach(Producer::close);
        pf.shutDown();
    }

    private static void assertOffsets(List<Future<RecordMetadata>> results)
            throws InterruptedException, ExecutionException {
        for (Future<RecordMetadata> result : results) {
            assertTrue(result.get().offset() >= 0);
        }
    }

    @Test
    void testCachingTransactionalProducerInstances() {
        ProducerFactory<String, String> producerFactory =
                transactionalProducerFactory(getBootstrapServers(), "bar");
        List<Producer<String, String>> testProducers = new ArrayList<>();
        testProducers.add(producerFactory.createProducer());

        Producer<String, String> firstProducer = testProducers.get(0);
        IntStream.range(0, 10).forEach(x -> {
            Producer<String, String> copy = producerFactory.createProducer();
            assertNotEquals(firstProducer, copy);
        });

        cleanup(producerFactory, testProducers);
    }

    @Test
    void testSendingMessagesUsingMultipleTransactionalProducers()
            throws ExecutionException, InterruptedException {
        ProducerFactory<String, String> producerFactory =
                transactionalProducerFactory(getBootstrapServers(), "xyz");
        List<Producer<String, String>> testProducers = new ArrayList<>();

        List<Future<RecordMetadata>> results = new ArrayList<>();
        IntStream.range(0, 10).forEach(x -> {
            Producer<String, String> producer = producerFactory.createProducer();
            producer.beginTransaction();
            results.add(send(producer, TOPIC, "foo" + x));
            producer.commitTransaction();
            testProducers.add(producer);
        });
        assertOffsets(results);

        cleanup(producerFactory, testProducers);
    }

    @Test
    void testClosingProducerShouldReturnItToCache() {
        ProducerFactory<Object, Object> pf = builder()
                .producerCacheSize(2)
                .configuration(minimalTransactional(getBootstrapServers()))
                .transactionalIdPrefix("cache")
                .build();
        Producer<Object, Object> first = pf.createProducer();
        first.close();
        Producer<Object, Object> second = pf.createProducer();
        second.close();
        assertEquals(first, second);
        pf.shutDown();
    }
}
