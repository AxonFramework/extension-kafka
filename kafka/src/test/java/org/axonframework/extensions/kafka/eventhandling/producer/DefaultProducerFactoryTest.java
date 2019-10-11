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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.axonframework.common.AxonConfigurationException;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode.NONE;
import static org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode.TRANSACTIONAL;
import static org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory.builder;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultProducerFactory}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        topics = {
                "testProducerCreation",
                "testSendingMessagesUsingMultipleProducers",
                "testSendingMessagesUsingMultipleTransactionalProducers",
                "testUsingCallbackWhilePublishingMessages",
                "testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction"
        },
        count = 3,
        ports = {0, 0, 0}
)
public class DefaultProducerFactoryTest {

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @Test
    public void testDefaultConfirmationMode() {
        assertThat(builder().configuration(empty()).build().confirmationMode()).isEqualTo(NONE);
    }

    @Test
    public void testDefaultConfirmationModeForTransactionalProducer() {
        assertThat(transactionalProducerFactory(kafkaBroker, "foo").confirmationMode()).isEqualTo(TRANSACTIONAL);
    }

    @Test(expected = AxonConfigurationException.class)
    public void testConfiguringInvalidCacheSize() {
        builder().configuration(minimal(kafkaBroker)).producerCacheSize(-1).build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testConfiguringInvalidTimeout() {
        builder().configuration(minimal(kafkaBroker)).closeTimeout(-1, ChronoUnit.SECONDS).build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testConfiguringInvalidTimeoutUnit() {
        builder().configuration(minimal(kafkaBroker)).closeTimeout(1, null).build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testConfiguringInvalidCloseTimeout() {
        builder().configuration(minimal(kafkaBroker)).closeTimeout(Duration.ofSeconds(-1)).build();
    }

    @Test
    public void testProducerCreation() {
        ProducerFactory<String, String> producerFactory = producerFactory(kafkaBroker);
        Producer<String, String> testProducer = producerFactory.createProducer();

        assertThat(testProducer.metrics()).isNotEmpty();
        assertThat(testProducer.partitionsFor("testProducerCreation")).isNotEmpty();

        cleanup(producerFactory, testProducer);
    }

    @Test
    public void testCachingProducerInstances() {
        ProducerFactory<String, String> producerFactory = producerFactory(kafkaBroker);
        List<Producer<String, String>> testProducers = new ArrayList<>();
        testProducers.add(producerFactory.createProducer());

        Producer<String, String> firstProducer = testProducers.get(0);
        IntStream.range(0, 10).forEach(x -> {
            Producer<String, String> copy = producerFactory.createProducer();
            assertThat(copy).isEqualTo(firstProducer);
            testProducers.add(copy);
        });

        cleanup(producerFactory, testProducers);
    }

    @Test
    public void testSendingMessagesUsingMultipleProducers() throws ExecutionException, InterruptedException {
        ProducerFactory<String, String> producerFactory = producerFactory(kafkaBroker);
        List<Producer<String, String>> testProducers = new ArrayList<>();
        String testTopic = "testSendingMessagesUsingMultipleProducers";

        List<Future<RecordMetadata>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Producer<String, String> producer = producerFactory.createProducer();
            results.add(send(producer, testTopic, "foo" + i));
            testProducers.add(producer);
        }
        assertOffsets(results);

        cleanup(producerFactory, testProducers);
    }

    @Test
    public void testTransactionalProducerCreation() {
        assumeFalse(
                "Transactional producers not supported on Windows",
                System.getProperty("os.name").contains("Windows")
        );

        ProducerFactory<String, String> producerFactory = transactionalProducerFactory(kafkaBroker, "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        testProducer.beginTransaction();
        testProducer.commitTransaction();
        assertThat(testProducer.metrics()).isNotEmpty();

        cleanup(producerFactory, testProducer);
    }

    @Test
    public void testCachingTransactionalProducerInstances() {
        ProducerFactory<String, String> producerFactory = transactionalProducerFactory(kafkaBroker, "bar");
        List<Producer<String, String>> testProducers = new ArrayList<>();
        testProducers.add(producerFactory.createProducer());

        Producer<String, String> firstProducer = testProducers.get(0);
        IntStream.range(0, 10).forEach(x -> {
            Producer<String, String> copy = producerFactory.createProducer();
            assertThat(copy).isNotEqualTo(firstProducer);
        });

        cleanup(producerFactory, testProducers);
    }

    @Test
    public void testSendingMessagesUsingMultipleTransactionalProducers()
            throws ExecutionException, InterruptedException {
        ProducerFactory<String, String> producerFactory = transactionalProducerFactory(kafkaBroker, "xyz");
        List<Producer<String, String>> testProducers = new ArrayList<>();
        String testTopic = "testSendingMessagesUsingMultipleTransactionalProducers";

        List<Future<RecordMetadata>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Producer<String, String> producer = producerFactory.createProducer();
            producer.beginTransaction();
            results.add(send(producer, testTopic, "foo" + i));
            producer.commitTransaction();
            testProducers.add(producer);
        }
        assertOffsets(results);

        cleanup(producerFactory, testProducers);
    }

    @Test(expected = KafkaException.class)
    public void testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction() {
        assumeFalse(
                "Transactional producers not supported on Windows",
                System.getProperty("os.name").contains("Windows")
        );

        ProducerFactory<String, String> producerFactory = transactionalProducerFactory(kafkaBroker, "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        try {
            testProducer.beginTransaction();
            send(testProducer, "testTransactionalProducerBehaviorOnCommittingAnAbortedTransaction", "bar");
            testProducer.abortTransaction();
            testProducer.commitTransaction();
        } finally {
            cleanup(producerFactory, testProducer);
        }
    }

    @Test(expected = KafkaException.class)
    public void testTransactionalProducerBehaviorOnSendingOffsetsWhenTransactionIsClosed() {
        assumeFalse(
                "Transactional producers not supported on Windows",
                System.getProperty("os.name").contains("Windows")
        );

        ProducerFactory<String, String> producerFactory = transactionalProducerFactory(kafkaBroker, "xyz");
        Producer<String, String> testProducer = producerFactory.createProducer();

        testProducer.beginTransaction();
        testProducer.commitTransaction();
        testProducer.sendOffsetsToTransaction(Collections.emptyMap(), "foo");

        cleanup(producerFactory, testProducer);
    }


    @Test
    public void testClosingProducerShouldReturnItToCache() {
        ProducerFactory<Object, Object> pf = builder()
                .producerCacheSize(2)
                .configuration(minimalTransactional(kafkaBroker))
                .transactionalIdPrefix("cache")
                .build();
        Producer<Object, Object> first = pf.createProducer();
        first.close();
        Producer<Object, Object> second = pf.createProducer();
        second.close();
        assertThat(second).isEqualTo(first);
        pf.shutDown();
    }

    @Test
    public void testUsingCallbackWhilePublishingMessages() throws ExecutionException, InterruptedException {
        Callback cb = mock(Callback.class);
        ProducerFactory<String, String> pf = producerFactory(kafkaBroker);
        Producer<String, String> producer = pf.createProducer();
        producer.send(new ProducerRecord<>("testUsingCallbackWhilePublishingMessages", "callback"), cb).get();
        producer.flush();
        verify(cb, only()).onCompletion(any(RecordMetadata.class), any());
        cleanup(pf, producer);
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
            assertThat(result.get().offset()).isGreaterThanOrEqualTo(0);
        }
    }
}
