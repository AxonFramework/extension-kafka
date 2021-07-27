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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode.NONE;
import static org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode.TRANSACTIONAL;
import static org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory.builder;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultProducerFactory}.
 *
 * @author Steven van Beelen
 * @author Nakul Mishra
 */
class DefaultProducerFactoryTest extends KafkaContainerTest {

    private static final String[] TOPICS = {
            "testProducerCreation",
            "testSendingMessagesUsingMultipleProducers",
            "testUsingCallbackWhilePublishingMessages",
            "testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction"};

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TOPICS);
    }

    @AfterAll
    public static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPICS);
    }

    private static Future<RecordMetadata> send(Producer<String, String> producer, String topic, String message) {
        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, message));
        producer.flush();
        return result;
    }

    private static void cleanup(ProducerFactory<String, String> pf, Producer<String, String> producer) {
        cleanup(pf, Collections.singletonList(producer));
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
    void testDefaultConfirmationMode() {
        assertEquals(builder().configuration(empty()).build().confirmationMode(), NONE);
    }

    @Test
    void testDefaultConfirmationModeForTransactionalProducer() {
        assertEquals(transactionalProducerFactory(getBootstrapServers(), "foo").confirmationMode(),
                     TRANSACTIONAL);
    }

    @Test
    void testConfiguringInvalidCacheSize() {
        assertThrows(
                AxonConfigurationException.class,
                () -> builder().configuration(minimal(getBootstrapServers())).producerCacheSize(-1)
                               .build()
        );
    }

    @Test
    void testConfiguringInvalidTimeout() {
        assertThrows(
                AxonConfigurationException.class,
                () -> builder().configuration(minimal(getBootstrapServers()))
                               .closeTimeout(-1, ChronoUnit.SECONDS).build()
        );
    }

    @Test
    void testConfiguringInvalidTimeoutUnit() {
        assertThrows(
                AxonConfigurationException.class,
                () -> builder().configuration(minimal(getBootstrapServers())).closeTimeout(1, null)
                               .build()
        );
    }

    @Test
    void testConfiguringInvalidCloseTimeout() {
        assertThrows(
                AxonConfigurationException.class,
                () -> builder().configuration(minimal(getBootstrapServers()))
                               .closeTimeout(Duration.ofSeconds(-1)).build()
        );
    }

    @Test
    void testConfiguringInvalidTransactionalIdPrefix() {
        assertThrows(
                AxonConfigurationException.class,
                () -> builder().transactionalIdPrefix(null).build()
        );
    }

    @Test
    void testProducerCreation() {
        ProducerFactory<String, String> producerFactory = producerFactory(getBootstrapServers());
        Producer<String, String> testProducer = producerFactory.createProducer();

        assertFalse(testProducer.metrics().isEmpty());
        assertFalse(testProducer.partitionsFor("testProducerCreation").isEmpty());

        cleanup(producerFactory, testProducer);
    }

    @Test
    void testCachingProducerInstances() {
        ProducerFactory<String, String> producerFactory = producerFactory(getBootstrapServers());
        List<Producer<String, String>> testProducers = new ArrayList<>();
        testProducers.add(producerFactory.createProducer());

        Producer<String, String> firstProducer = testProducers.get(0);
        IntStream.range(0, 10).forEach(x -> {
            Producer<String, String> copy = producerFactory.createProducer();
            assertEquals(firstProducer, copy);
            testProducers.add(copy);
        });

        cleanup(producerFactory, testProducers);
    }

    @Test
    void testSendingMessagesUsingMultipleProducers() throws ExecutionException, InterruptedException {
        ProducerFactory<String, String> producerFactory = producerFactory(getBootstrapServers());
        List<Producer<String, String>> testProducers = new ArrayList<>();
        String testTopic = "testSendingMessagesUsingMultipleProducers";

        List<Future<RecordMetadata>> results = new ArrayList<>();
        // The reason we are looping 12 times is a bug we used to have where the (producerCacheSize + 2)-th send failed because the producer was closed.
        // To avoid regression, we keep the test like this.
        IntStream.range(0, 12).forEach(x -> {
            Producer<String, String> producer = producerFactory.createProducer();
            results.add(send(producer, testTopic, "foo" + x));
            producer.close();
            testProducers.add(producer);
        });
        assertOffsets(results);

        cleanup(producerFactory, testProducers);
    }

    @Test
    void testTransactionalProducerCreation() {
        assumeFalse(
                System.getProperty("os.name").contains("Windows"),
                "Transactional producers not supported on Windows"
        );

        ProducerFactory<String, String> producerFactory =
                transactionalProducerFactory(getBootstrapServers(), "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        testProducer.beginTransaction();
        testProducer.commitTransaction();
        assertFalse(testProducer.metrics().isEmpty());

        cleanup(producerFactory, testProducer);
    }

    @Test
    void testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction() {
        assumeFalse(
                System.getProperty("os.name").contains("Windows"),
                "Transactional producers not supported on Windows"
        );

        ProducerFactory<String, String> producerFactory =
                transactionalProducerFactory(getBootstrapServers(), "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        try {
            testProducer.beginTransaction();
            send(testProducer, "testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction", "bar");
            testProducer.abortTransaction();
            assertThrows(KafkaException.class, testProducer::commitTransaction);
        } finally {
            cleanup(producerFactory, testProducer);
        }
    }

    @Test
    void testTransactionalProducerBehaviorOnSendingOffsetsWhenTransactionIsClosed() {
        assumeFalse(
                System.getProperty("os.name").contains("Windows"),
                "Transactional producers not supported on Windows"
        );

        ProducerFactory<String, String> producerFactory =
                transactionalProducerFactory(getBootstrapServers(), "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        testProducer.beginTransaction();
        testProducer.commitTransaction();
        assertThrows(KafkaException.class, () -> testProducer.sendOffsetsToTransaction(Collections.emptyMap(), "foo"));

        cleanup(producerFactory, testProducer);
    }

    @Test
    void testUsingCallbackWhilePublishingMessages() throws ExecutionException, InterruptedException {
        Callback cb = mock(Callback.class);
        ProducerFactory<String, String> pf = producerFactory(getBootstrapServers());
        Producer<String, String> producer = pf.createProducer();
        producer.send(new ProducerRecord<>("testUsingCallbackWhilePublishingMessages", "callback"), cb).get();
        producer.flush();
        verify(cb, only()).onCompletion(any(RecordMetadata.class), any());
        cleanup(pf, producer);
    }
}
