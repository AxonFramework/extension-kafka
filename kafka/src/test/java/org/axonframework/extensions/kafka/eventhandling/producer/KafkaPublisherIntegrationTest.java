/*
 * Copyright (c) 2010-2022. Axon Framework
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.axonframework.extensions.kafka.utils.TestSerializer;
import org.axonframework.messaging.EventPublicationFailedException;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.monitoring.MessageMonitor;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher.DEFAULT_PROCESSING_GROUP;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.transactionalConsumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.KafkaTestUtils.getRecords;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.ackProducerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.transactionalProducerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link KafkaPublisher} asserting utilization of the class.
 *
 * @author Steven van Beelen
 * @author Nakul Mishra
 */
class KafkaPublisherIntegrationTest extends KafkaContainerTest {

    private static final String[] TOPICS = {
            "testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully",
            "testSendMessagesAckNoUnitOfWorkWithTimeout",
            "testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully",
            "testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully",
            "testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully",
            "testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished",
            "testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException",
            "testPublishMessageKafkaTransactionCannotBeCommittedShouldNotPublishEvents",
            "testSendMessageWithKafkaTransactionRollback"};
    private Configurer configurer;
    private SimpleEventBus eventBus;
    private MessageCollector monitor;
    private ConsumerFactory<String, Object> consumerFactory;
    private Consumer<?, ?> testConsumer;
    private ProducerFactory<String, byte[]> testProducerFactory;
    private KafkaPublisher<String, byte[]> testSubject;

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TOPICS);
    }

    @AfterAll
    static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TOPICS);
    }

    @SuppressWarnings("unchecked")
    private static DefaultProducerFactory<String, byte[]> producerFactoryWithFencedExceptionOnBeginTransaction() {
        DefaultProducerFactory<String, byte[]> producerFactory =
                mock(DefaultProducerFactory.class, "FactoryForExceptionOnBeginTx");
        Producer<String, byte[]> producer = mock(Producer.class, "ExceptionOnBeginTxMock");
        when(producerFactory.confirmationMode()).thenReturn(ConfirmationMode.TRANSACTIONAL);
        when(producerFactory.createProducer()).thenReturn(producer);
        doThrow(ProducerFencedException.class).when(producer).beginTransaction();
        return producerFactory;
    }

    @SuppressWarnings("unchecked")
    private static DefaultProducerFactory<String, byte[]> producerFactoryWithFencedExceptionOnCommit() {
        DefaultProducerFactory<String, byte[]> producerFactory = mock(DefaultProducerFactory.class);
        Producer<String, byte[]> producer = mock(Producer.class, "ExceptionOnCommitTxMock");
        when(producerFactory.confirmationMode()).thenReturn(ConfirmationMode.TRANSACTIONAL);
        when(producerFactory.createProducer()).thenReturn(producer);
        doThrow(ProducerFencedException.class).when(producer).commitTransaction();
        return producerFactory;
    }

    @SuppressWarnings("unchecked")
    private static DefaultProducerFactory<String, byte[]> producerFactoryWithFencedExceptionOnAbort() {
        DefaultProducerFactory<String, byte[]> producerFactory =
                mock(DefaultProducerFactory.class, "FactoryForExceptionOnAbortTx");
        Producer<String, byte[]> producer = mock(Producer.class, "ExceptionOnAbortTx");
        when(producerFactory.confirmationMode()).thenReturn(ConfirmationMode.TRANSACTIONAL);
        when(producerFactory.createProducer()).thenReturn(producer);
        doThrow(RuntimeException.class).when(producer).abortTransaction();
        return producerFactory;
    }

    private static List<GenericDomainEventMessage<String>> domainMessages(String aggregateId, int limit) {
        return IntStream.range(0, limit)
                        .mapToObj(i -> domainMessage(aggregateId))
                        .collect(Collectors.toList());
    }

    private static GenericDomainEventMessage<String> domainMessage(String aggregateId) {
        return new GenericDomainEventMessage<>("Stub", aggregateId, 1L, "Payload", MetaData.with("key", "value"));
    }

    @BeforeEach
    void setUp() {
        this.configurer = DefaultConfigurer.defaultConfiguration();
        this.eventBus = SimpleEventBus.builder().build();
        this.monitor = new MessageCollector();
        this.configurer.configureEventBus(configuration -> eventBus);
        this.consumerFactory = transactionalConsumerFactory(getBootstrapServers(),
                                                            ByteArrayDeserializer.class);
        this.testConsumer = mock(Consumer.class);
    }

    @AfterEach
    void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
        this.monitor.reset();
        this.testConsumer.close();

        // the KafkaPublisher.Builder tests do not set the regular test subjects
        if (Objects.nonNull(testSubject)) {
            this.testSubject.shutDown();
            this.testProducerFactory.shutDown();
        }
    }

    @Test
    void testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        String testTopic = "testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = ackProducerFactory(getBootstrapServers(), ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        int numberOfMessages = 10;
        List<GenericDomainEventMessage<String>> testMessages = domainMessages("1234", numberOfMessages);

        eventBus.publish(testMessages);

        ConsumerRecords<?, ?> consumedRecords = getRecords(testConsumer, 60000, numberOfMessages);
        assertEquals(numberOfMessages, consumedRecords.count());
        assertEquals(testMessages, monitor.getReceived());
        assertEquals(0, monitor.failureCount());
        assertEquals(0, monitor.ignoreCount());
        assertEquals(numberOfMessages, monitor.successCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPublishingMessageAckModeNoUnitOfWorkButKafkaReturnTimeoutOnWriteShouldBeMarkedAsFailed() {
        String testTopic = "testSendMessagesAckNoUnitOfWorkWithTimeout";
        testProducerFactory = mock(DefaultProducerFactory.class);
        when(testProducerFactory.confirmationMode()).thenReturn(ConfirmationMode.WAIT_FOR_ACK);
        Producer<String, byte[]> testProducer = mock(Producer.class);
        when(testProducerFactory.createProducer()).thenReturn(testProducer);

        Future<RecordMetadata> timeoutFuture1 = new CompletableFuture<>();
        Future<RecordMetadata> timeoutFuture2 = new CompletableFuture<>();
        when(testProducer.send(any())).thenReturn(timeoutFuture1).thenReturn(timeoutFuture2);

        testSubject = buildPublisher(testTopic);
        List<GenericDomainEventMessage<String>> testMessages = domainMessages("98765", 2);

        assertThrows(EventPublicationFailedException.class, () -> eventBus.publish(testMessages));
        assertEquals(0, monitor.successCount());
        assertEquals(1, monitor.failureCount());
        assertEquals(0, monitor.ignoreCount());
    }

    @Test
    void testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        assumeFalse(
                System.getProperty("os.name").contains("Windows"),
                "Transactional producers not supported on Windows"
        );

        String testTopic = "testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = transactionalProducerFactory(getBootstrapServers(), "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);
        List<GenericDomainEventMessage<String>> testMessages = domainMessages("62457", 5);

        eventBus.publish(testMessages);

        assertEquals(testMessages.size(), monitor.successCount());
        assertEquals(0, monitor.failureCount());
        assertEquals(0, monitor.ignoreCount());
        assertEquals(testMessages.size(), getRecords(testConsumer).count());
    }

    @Test
    void testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        String testTopic = "testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = ackProducerFactory(getBootstrapServers(), ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("1234");

        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        eventBus.publish(testMessage);
        uow.commit();

        assertEquals(monitor.getReceived(), singletonList(testMessage));
        assertEquals(1, monitor.successCount());
        assertEquals(0, monitor.failureCount());
        assertEquals(0, monitor.ignoreCount());
        assertEquals(1, getRecords(testConsumer).count());
    }

    @Test
    void testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        assumeFalse(
                System.getProperty("os.name").contains("Windows"),
                "Transactional producers not supported on Windows"
        );

        String testTopic = "testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = transactionalProducerFactory(getBootstrapServers(), "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        GenericDomainEventMessage<String> testMessage = domainMessage("121");

        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        eventBus.publish(testMessage);
        uow.commit();

        assertEquals(1, getRecords(testConsumer).count());
    }

    @Test
    void testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished() {
        String expectedException = "Some exception";

        String testTopic = "testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished";
        testProducerFactory = transactionalProducerFactory(getBootstrapServers(), "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        GenericDomainEventMessage<String> testMessage = domainMessage("123456");
        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        uow.onPrepareCommit(u -> {
            throw new RuntimeException(expectedException);
        });

        eventBus.publish(testMessage);

        try {
            uow.commit();
            fail("Expected a RuntimeException to be thrown");
        } catch (Exception e) {
            assertEquals(expectedException, e.getMessage());
        }

        assertTrue(getRecords(testConsumer, 100).isEmpty(), "Didn't expect any consumer records");
    }

    @Test
    void testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException() {
        String testTopic = "testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException";
        testProducerFactory = producerFactoryWithFencedExceptionOnBeginTransaction();
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("500");

        assertThrows(EventPublicationFailedException.class, () -> publishWithException(testTopic, testMessage));
    }

    @Test
    void testPublishMessageKafkaTransactionCannotBeCommittedShouldThrowAnException() {
        String testTopic = "testPublishMessageKafkaTransactionCannotBeCommittedShouldNotPublishEvents";
        testProducerFactory = producerFactoryWithFencedExceptionOnCommit();
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("9000");

        assertThrows(EventPublicationFailedException.class, () -> publishWithException(testTopic, testMessage));
    }

    @Test
    void testSendMessageWithKafkaTransactionRollback() {
        String expectedException = "Some exception";

        String testTopic = "testSendMessageWithKafkaTransactionRollback";
        testProducerFactory = producerFactoryWithFencedExceptionOnAbort();
        testSubject = buildPublisher(testTopic);

        GenericDomainEventMessage<String> testMessage = domainMessage("76123");
        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        uow.onPrepareCommit(u -> {
            throw new RuntimeException(expectedException);
        });

        eventBus.publish(testMessage);

        try {
            uow.commit();
            fail("Expected a RuntimeException to be thrown");
        } catch (Exception e) {
            assertEquals(expectedException, e.getMessage());
        }

        testConsumer = buildConsumer(testTopic);
        assertTrue(getRecords(testConsumer, 100).isEmpty(), "Didn't expect any consumer records");
    }

    @Test
    void testConfiguringInvalidProducerFactory() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.producerFactory(null));
    }

    @Test
    void testConfiguringInvalidMessageConverter() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.messageConverter(null));
    }

    @Test
    void testConfiguringInvalidMessageMonitor() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.messageMonitor(null));
    }

    @Test
    void testConfiguringInvalidKafkaTopic() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.topic(null));
    }

    @Test
    void testConfiguringInvalidKafkaTopicFunction() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.topicResolver(null));
    }

    @Test
    void testConfiguringInvalidAckTimeout() {
        KafkaPublisher.Builder<Object, Object> builderTestSubject = KafkaPublisher.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.publisherAckTimeout(-12));
    }

    private KafkaPublisher<String, byte[]> buildPublisher(String topic) {
        DefaultKafkaMessageConverter messageConverter =
                DefaultKafkaMessageConverter.builder()
                                            .serializer(TestSerializer.XSTREAM.getSerializer())
                                            .build();
        KafkaPublisher<String, byte[]> kafkaPublisher = KafkaPublisher.<String, byte[]>builder()
                                                                      .producerFactory(testProducerFactory)
                                                                      .messageConverter(messageConverter)
                                                                      .messageMonitor(monitor)
                                                                      .topicResolver(m -> Optional.of(topic))
                                                                      .publisherAckTimeout(1000)
                                                                      .build();
        KafkaEventPublisher<String, byte[]> kafkaEventPublisher =
                KafkaEventPublisher.<String, byte[]>builder().kafkaPublisher(kafkaPublisher).build();
        /*
         * Simulate configuration.
         * - Use SubscribingEventProcessor for simplicity
         * - Use KafkaEventPublisher
         * - Since it uses a processor, it will catch exceptions, so register the corresponding
         */
        configurer.eventProcessing(eventProcessingConfigurer -> eventProcessingConfigurer
                .registerEventHandler(config -> kafkaEventPublisher)

                .registerListenerInvocationErrorHandler(
                        DEFAULT_PROCESSING_GROUP, config -> PropagatingErrorHandler.instance()
                )
                .registerSubscribingEventProcessor(DEFAULT_PROCESSING_GROUP)
                .assignHandlerInstancesMatching(
                        DEFAULT_PROCESSING_GROUP,
                        eventHandler -> eventHandler.getClass().equals(KafkaEventPublisher.class)
                )
        ).start();

        return kafkaPublisher;
    }

    private Consumer<?, ?> buildConsumer(String topic) {
        Consumer<?, ?> consumer = consumerFactory.createConsumer(DEFAULT_GROUP_ID);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    private void publishWithException(String topic, GenericDomainEventMessage<String> message) {
        try {
            UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(message);
            eventBus.publish(message);
            uow.commit();
        } finally {
            testConsumer = buildConsumer(topic);
            assertTrue(getRecords(testConsumer, 100).isEmpty(), "Didn't expect any consumer records");
        }
    }

    private static class MessageCollector implements MessageMonitor<Message<?>> {

        private final AtomicInteger successCounter = new AtomicInteger(0);
        private final AtomicInteger failureCounter = new AtomicInteger(0);
        private final AtomicInteger ignoreCounter = new AtomicInteger(0);

        private final List<Message<?>> received = new CopyOnWriteArrayList<>();

        @Override
        public MonitorCallback onMessageIngested(Message<?> message) {
            received.add(message);
            return new MonitorCallback() {
                @Override
                public void reportSuccess() {
                    successCounter.incrementAndGet();
                }

                @Override
                public void reportFailure(Throwable cause) {
                    failureCounter.incrementAndGet();
                }

                @Override
                public void reportIgnored() {
                    ignoreCounter.incrementAndGet();
                }
            };
        }

        int successCount() {
            return successCounter.get();
        }

        int failureCount() {
            return failureCounter.get();
        }

        int ignoreCount() {
            return ignoreCounter.get();
        }

        List<Message<?>> getReceived() {
            return received;
        }

        void reset() {
            received.clear();
        }
    }
}
