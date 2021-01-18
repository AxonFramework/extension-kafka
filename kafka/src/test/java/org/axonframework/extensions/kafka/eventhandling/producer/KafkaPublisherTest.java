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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.messaging.EventPublicationFailedException;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher.DEFAULT_PROCESSING_GROUP;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.transactionalConsumerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.ackProducerFactory;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.transactionalProducerFactory;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link KafkaPublisher} asserting utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        topics = {
                "testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully",
                "testSendMessagesAckNoUnitOfWorkWithTimeout",
                "testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully",
                "testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully",
                "testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully",
                "testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished",
                "testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException",
                "testPublishMessageKafkaTransactionCannotBeCommittedShouldNotPublishEvents",
                "testSendMessageWithKafkaTransactionRollback"
        },
        count = 3,
        ports = {0, 0, 0}
)
public class KafkaPublisherTest {

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;
    private Configurer configurer;
    private SimpleEventBus eventBus;
    private MessageCollector monitor;
    private ConsumerFactory<String, Object> consumerFactory;
    private Consumer<?, ?> testConsumer;

    private ProducerFactory<String, byte[]> testProducerFactory;
    private KafkaPublisher<String, byte[]> testSubject;

    @Before
    public void setUp() {
        this.configurer = DefaultConfigurer.defaultConfiguration();
        this.eventBus = SimpleEventBus.builder().build();
        this.monitor = new MessageCollector();
        this.configurer.configureEventBus(configuration -> eventBus);
        this.consumerFactory = transactionalConsumerFactory(kafkaBroker, ByteArrayDeserializer.class);
        this.testConsumer = mock(Consumer.class);
    }

    @After
    public void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
        this.monitor.reset();
        this.testConsumer.close();
        this.testSubject.shutDown();
        this.testProducerFactory.shutDown();
    }

    @Test
    public void testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        String testTopic = "testPublishMessagesWithAckModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = ackProducerFactory(kafkaBroker, ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        int numberOfMessages = 10;
        List<GenericDomainEventMessage<String>> testMessages = domainMessages("1234", numberOfMessages);

        eventBus.publish(testMessages);

        ConsumerRecords<?, ?> consumedRecords = KafkaTestUtils.getRecords(testConsumer, 60000, numberOfMessages);
        assertThat(consumedRecords.count()).isEqualTo(numberOfMessages);
        assertThat(testMessages).isEqualTo(monitor.getReceived());
        assertThat(monitor.failureCount()).isZero();
        assertThat(monitor.ignoreCount()).isZero();
        assertThat(monitor.successCount()).isEqualTo(numberOfMessages);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPublishingMessageAckModeNoUnitOfWorkButKafkaReturnTimeoutOnWriteShouldBeMarkedAsFailed() {
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

        assertThatExceptionOfType(EventPublicationFailedException.class)
                .isThrownBy(() -> eventBus.publish(testMessages));
        assertThat(monitor.successCount()).isZero();
        assertThat(monitor.failureCount()).isEqualTo(1);
        assertThat(monitor.ignoreCount()).isZero();
    }

    @Test
    public void testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        assumeFalse(
                "Transactional producers not supported on Windows",
                System.getProperty("os.name").contains("Windows")
        );

        String testTopic = "testPublishMessagesWithTransactionalModeNoUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = transactionalProducerFactory(kafkaBroker, "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);
        List<GenericDomainEventMessage<String>> testMessages = domainMessages("62457", 5);

        eventBus.publish(testMessages);

        assertThat(monitor.successCount()).isEqualTo(testMessages.size());
        assertThat(monitor.failureCount()).isZero();
        assertThat(monitor.ignoreCount()).isZero();
        assertThat(KafkaTestUtils.getRecords(testConsumer).count()).isEqualTo(testMessages.size());
    }

    @Test
    public void testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        String testTopic = "testPublishMessagesWithAckModeUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = ackProducerFactory(kafkaBroker, ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("1234");

        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        eventBus.publish(testMessage);
        uow.commit();

        assertThat(singletonList(testMessage)).isEqualTo(monitor.getReceived());
        assertThat(monitor.successCount()).isOne();
        assertThat(monitor.failureCount()).isZero();
        assertThat(monitor.ignoreCount()).isZero();
        assertThat(KafkaTestUtils.getRecords(testConsumer).count()).isOne();
    }

    @Test
    public void testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully() {
        assumeFalse(
                "Transactional producers not supported on Windows",
                System.getProperty("os.name").contains("Windows")
        );

        String testTopic = "testPublishMessagesWithTransactionalModeUnitOfWorkShouldBePublishedAndReadSuccessfully";
        testProducerFactory = transactionalProducerFactory(kafkaBroker, "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        GenericDomainEventMessage<String> testMessage = domainMessage("121");

        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        eventBus.publish(testMessage);
        uow.commit();

        assertThat(KafkaTestUtils.getRecords(testConsumer).count()).isOne();
    }

    @Test
    public void testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished() {
        String expectedException = "Some exception";

        String testTopic = "testPublishMessageWithTransactionalModeUnitOfWorkRollbackShouldNeverBePublished";
        testProducerFactory = transactionalProducerFactory(kafkaBroker, "foo", ByteArraySerializer.class);
        testConsumer = buildConsumer(testTopic);
        testSubject = buildPublisher(testTopic);

        GenericDomainEventMessage<String> testMessage = domainMessage("123456");
        UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(testMessage);
        uow.onPrepareCommit(u -> {
            throw new RuntimeException(expectedException);
        });

        eventBus.publish(testMessage);

        //noinspection CatchMayIgnoreException
        try {
            uow.commit();
            fail("Expected a RuntimeException to be thrown");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo(expectedException);
        }

        assertTrue("Didn't expect any consumer records", KafkaTestUtils.getRecords(testConsumer, 100).isEmpty());
    }

    @Test(expected = EventPublicationFailedException.class)
    public void testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException() {
        String testTopic = "testPublishMessagesKafkaTransactionCannotBeStartedShouldThrowAnException";
        testProducerFactory = producerFactoryWithFencedExceptionOnBeginTransaction();
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("500");

        publishWithException(testTopic, testMessage);
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

    @Test(expected = EventPublicationFailedException.class)
    public void testPublishMessageKafkaTransactionCannotBeCommittedShouldThrowAnException() {
        String testTopic = "testPublishMessageKafkaTransactionCannotBeCommittedShouldNotPublishEvents";
        testProducerFactory = producerFactoryWithFencedExceptionOnCommit();
        testSubject = buildPublisher(testTopic);
        GenericDomainEventMessage<String> testMessage = domainMessage("9000");

        publishWithException(testTopic, testMessage);
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

    @Test
    public void testSendMessageWithKafkaTransactionRollback() {
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

        //noinspection CatchMayIgnoreException
        try {
            uow.commit();
            fail("Expected a RuntimeException to be thrown");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo(expectedException);
        }

        testConsumer = buildConsumer(testTopic);
        assertTrue("Didn't expect any consumer records", KafkaTestUtils.getRecords(testConsumer, 100).isEmpty());
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

    private KafkaPublisher<String, byte[]> buildPublisher(String topic) {
        DefaultKafkaMessageConverter messageConverter =
                DefaultKafkaMessageConverter.builder()
                                            .serializer(XStreamSerializer.builder().build())
                                            .build();
        KafkaPublisher<String, byte[]> kafkaPublisher = KafkaPublisher.<String, byte[]>builder()
                .producerFactory(testProducerFactory)
                .messageConverter(messageConverter)
                .messageMonitor(monitor)
                .topic(topic)
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

    private static List<GenericDomainEventMessage<String>> domainMessages(String aggregateId, int limit) {
        return IntStream.range(0, limit)
                        .mapToObj(i -> domainMessage(aggregateId))
                        .collect(Collectors.toList());
    }

    private static GenericDomainEventMessage<String> domainMessage(String aggregateId) {
        return new GenericDomainEventMessage<>("Stub", aggregateId, 1L, "Payload", MetaData.with("key", "value"));
    }

    private void publishWithException(String topic, GenericDomainEventMessage<String> message) {
        try {
            UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(message);
            eventBus.publish(message);
            uow.commit();
        } finally {
            testConsumer = buildConsumer(topic);
            assertTrue("Didn't expect any consumer records", KafkaTestUtils.getRecords(testConsumer, 100).isEmpty());
        }
    }

    private static class MessageCollector implements MessageMonitor<Message<?>> {

        private final AtomicInteger successCounter = new AtomicInteger(0);
        private final AtomicInteger failureCounter = new AtomicInteger(0);
        private final AtomicInteger ignoreCounter = new AtomicInteger(0);

        private List<Message<?>> received = new CopyOnWriteArrayList<>();

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
