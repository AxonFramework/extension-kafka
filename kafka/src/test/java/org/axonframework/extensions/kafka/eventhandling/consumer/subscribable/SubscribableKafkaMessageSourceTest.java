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

package org.axonframework.extensions.kafka.eventhandling.consumer.subscribable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.TopicSubscriberBuilder;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.awaitility.Awaitility.await;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SubscribableKafkaMessageSource}, asserting construction and utilization of the class.
 *
 * @author Steven van Beelen
 */
class SubscribableKafkaMessageSourceTest {

    private static final String TEST_TOPIC = "someTopic";
    private static final Registration NO_OP_FETCHER_REGISTRATION = () -> {
        // No-op
        return true;
    };
    private static final java.util.function.Consumer<List<? extends EventMessage<?>>> NO_OP_EVENT_PROCESSOR = eventMessages -> {
        // No-op
    };

    private ConsumerFactory<String, String> consumerFactory;
    private Fetcher<String, String, EventMessage<?>> fetcher;

    private SubscribableKafkaMessageSource<String, String> testSubject;

    private Consumer<String, String> mockConsumer;

    abstract class SubscribableKafkaMessageSourceTestFixture {
        abstract <T extends TopicSubscriberBuilder<T>> T setTopic(TopicSubscriberBuilder<T> builder);
        void testStartBuildsConsumersUpToConsumerCount(int expectedNumberOfConsumers) {
            when(fetcher.poll(eq(mockConsumer), any(), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

            SubscribableKafkaMessageSource<String, String> testSubject =
                    setTopic(SubscribableKafkaMessageSource.<String, String>builder())
                            .groupId(DEFAULT_GROUP_ID)
                            .consumerFactory(consumerFactory)
                            .fetcher(fetcher)
                            .consumerCount(expectedNumberOfConsumers)
                            .build();

            testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
            testSubject.start();

            verify(consumerFactory, times(expectedNumberOfConsumers)).createConsumer(DEFAULT_GROUP_ID);
            verify(fetcher, times(expectedNumberOfConsumers)).poll(eq(mockConsumer), any(), any(), any());
        }

        void testCloseRunsCloseHandlerPerConsumerCount(int expectedNumberOfConsumers) {

            AtomicBoolean closedEventProcessorOne = new AtomicBoolean(false);
            AtomicBoolean closedEventProcessorTwo = new AtomicBoolean(false);
            when(fetcher.poll(eq(mockConsumer), any(), any(), any()))
                    .thenReturn(() -> {
                        closedEventProcessorOne.set(true);
                        return true;
                    })
                    .thenReturn(() -> {
                        closedEventProcessorTwo.set(true);
                        return true;
                    });

            SubscribableKafkaMessageSource<String, String> testSubject =
                    setTopic(SubscribableKafkaMessageSource.<String, String>builder())
                            .groupId(DEFAULT_GROUP_ID)
                            .consumerFactory(consumerFactory)
                            .fetcher(fetcher)
                            .autoStart()
                            .consumerCount(expectedNumberOfConsumers)
                            .build();

            testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
            testSubject.close();

            verify(consumerFactory, times(expectedNumberOfConsumers)).createConsumer(DEFAULT_GROUP_ID);
            verify(fetcher, times(expectedNumberOfConsumers)).poll(eq(mockConsumer), any(), any(), any());

            assertTrue(closedEventProcessorOne.get());
            assertTrue(closedEventProcessorTwo.get());
        }

        void restartingConsumerShouldNotCauseAMemoryLeakAndOnCloseNoRegistrationsShouldBeleft() throws NoSuchFieldException, IllegalAccessException {
            fetcher = AsyncFetcher.<String, String, EventMessage<?>>builder()
                    .executorService(newSingleThreadExecutor()).build();
            when(mockConsumer.poll(any(Duration.class))).thenThrow(new BrokerNotAvailableException("none available"));

            SubscribableKafkaMessageSource<String, String> testSubject =
                    setTopic(SubscribableKafkaMessageSource.<String, String>builder())
                            .groupId(DEFAULT_GROUP_ID)
                            .consumerFactory(consumerFactory)
                            .fetcher(fetcher)
                            .autoStart()
                            .build();

            testSubject.subscribe(NO_OP_EVENT_PROCESSOR);

            await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> verify(consumerFactory, atLeast(4)).createConsumer(DEFAULT_GROUP_ID));
            Field fetcherRegistrations = SubscribableKafkaMessageSource.class.getDeclaredField("fetcherRegistrations");

            fetcherRegistrations.setAccessible(true);

            Map<Integer, Registration> registrations = (Map<Integer, Registration>) fetcherRegistrations.get(testSubject);
            assertEquals(1, registrations.values().size());

            testSubject.close();

            registrations = (Map<Integer, Registration>) fetcherRegistrations.get(testSubject);
            assertTrue(registrations.isEmpty());
        }
    }

    class ListSubscribableKafkaMessageSourceTestFixture extends SubscribableKafkaMessageSourceTestFixture {
        @Override
        <T extends TopicSubscriberBuilder<T>> T setTopic(TopicSubscriberBuilder<T> builder) {
            return builder.topics(Collections.singletonList(TEST_TOPIC));
        }
    }

    class PatternSubscribableKafkaMessageSourceTestFixture extends SubscribableKafkaMessageSourceTestFixture {
        final private Pattern pattern;

        PatternSubscribableKafkaMessageSourceTestFixture(Pattern pattern) {
            this.pattern = pattern;

        }
        @Override
        <T extends TopicSubscriberBuilder<T>> T setTopic(TopicSubscriberBuilder<T> builder) {
            return builder.topicPattern(pattern);
        }
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        consumerFactory = mock(ConsumerFactory.class);
        mockConsumer = mock(Consumer.class);
        when(consumerFactory.createConsumer(DEFAULT_GROUP_ID)).thenReturn(mockConsumer);
        fetcher = mock(Fetcher.class);

        testSubject = SubscribableKafkaMessageSource.<String, String>builder()
                                                    .topics(Collections.singletonList(TEST_TOPIC))
                                                    .groupId(DEFAULT_GROUP_ID)
                                                    .consumerFactory(consumerFactory)
                                                    .fetcher(fetcher)
                                                    .build();
    }

    @AfterEach
    void tearDown() {
        testSubject.close();
    }

    @Test
    void testBuildWithInvalidTopicsThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.topics(null));
    }

    @Test
    void testBuildWithInvalidTopicThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.addTopic(null));
    }

    @Test
    void testBuildWithInvalidGroupIdThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.groupId(null));
    }

    @Test
    void testBuildWithInvalidConsumerFactoryThrowsAxonConfigurationException() {
        //noinspection unchecked,rawtypes
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(
                AxonConfigurationException.class,
                () -> builder.consumerFactory((ConsumerFactory) null)
        );
    }

    @Test
    void testBuildWithInvalidFetcherThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.fetcher(null));
    }

    @Test
    void testBuildWithInvalidMessageConverterThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.messageConverter(null));
    }

    @Test
    void testBuildWithInvalidConsumerCountThrowsAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.consumerCount(0));
    }

    @Test
    void testBuildingWhilstMissingRequiredFieldsShouldThrowAxonConfigurationException() {
        SubscribableKafkaMessageSource.Builder<Object, Object> builder = SubscribableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, builder::build);
    }

    @Test
    void testAutoStartInitiatesProcessingOnFirstEventProcessor() {
        when(fetcher.poll(eq(mockConsumer), any(), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        SubscribableKafkaMessageSource<String, String> testSubject =
                SubscribableKafkaMessageSource.<String, String>builder()
                                              .topics(Collections.singletonList(TEST_TOPIC))
                                              .groupId(DEFAULT_GROUP_ID)
                                              .consumerFactory(consumerFactory)
                                              .fetcher(fetcher)
                                              .autoStart()
                                              .build();

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);

        verify(consumerFactory, times(1)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(1)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher).poll(eq(mockConsumer), any(), any(), any());
    }

    @Test
    void testCancelingSubscribedEventProcessorRunsConnectedCloseHandlerWhenAutoStartIsOn() {
        AtomicBoolean closedFetcherRegistration = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any(), any())).thenReturn(() -> {
            closedFetcherRegistration.set(true);
            return true;
        });

        SubscribableKafkaMessageSource<String, String> testSubject =
                SubscribableKafkaMessageSource.<String, String>builder()
                                              .topics(Collections.singletonList(TEST_TOPIC))
                                              .groupId(DEFAULT_GROUP_ID)
                                              .consumerFactory(consumerFactory)
                                              .fetcher(fetcher)
                                              .autoStart() // This enables auto close
                                              .build();

        Registration registration = testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.start();

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer).subscribe(Collections.singletonList(TEST_TOPIC));

        assertTrue(registration.cancel());
        assertTrue(closedFetcherRegistration.get());
    }

    @Test
    void testSubscribingTheSameInstanceTwiceDisregardsSecondInstanceOnStart() {
        when(fetcher.poll(eq(mockConsumer), any(), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);

        testSubject.start();

        verify(consumerFactory, times(1)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(1)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher, times(1)).poll(eq(mockConsumer), any(), any(), any());
    }

    @Test
    void testStartSubscribesConsumerToAllProvidedTopics() {
        when(fetcher.poll(eq(mockConsumer), any(), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        List<String> testTopics = new ArrayList<>();
        testTopics.add("topicOne");
        testTopics.add("topicTwo");

        SubscribableKafkaMessageSource<String, String> testSubject =
                SubscribableKafkaMessageSource.<String, String>builder()
                                              .topics(testTopics)
                                              .groupId(DEFAULT_GROUP_ID)
                                              .consumerFactory(consumerFactory)
                                              .fetcher(fetcher)
                                              .build();

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.start();

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer).subscribe(testTopics);
        verify(fetcher).poll(eq(mockConsumer), any(), any(), any());
    }

    @Test
    void testStartBuildsConsumersUpToConsumerCountUsingListBasedSubscription() {
        int expectedNumberOfConsumers = 2;
        new ListSubscribableKafkaMessageSourceTestFixture().testStartBuildsConsumersUpToConsumerCount(expectedNumberOfConsumers);
        verify(mockConsumer, times(expectedNumberOfConsumers)).subscribe(Collections.singletonList(TEST_TOPIC));
    }

    @Test
    void testCloseRunsCloseHandlerPerConsumerCountUsingListBasedSubscription() {
        int expectedNumberOfConsumers = 2;
        new ListSubscribableKafkaMessageSourceTestFixture().testCloseRunsCloseHandlerPerConsumerCount(expectedNumberOfConsumers);
        verify(mockConsumer, times(expectedNumberOfConsumers)).subscribe(Collections.singletonList(TEST_TOPIC));
    }

    @Test
    void restartingConsumerShouldNotCauseAMemoryLeakAndOnCloseNoRegistrationsShouldBeleftUsingListBasedSubscription() throws NoSuchFieldException, IllegalAccessException {
        new ListSubscribableKafkaMessageSourceTestFixture().restartingConsumerShouldNotCauseAMemoryLeakAndOnCloseNoRegistrationsShouldBeleft();
    }
    @Test
    void testStartBuildsConsumersUpToConsumerCountUsingPatternBasedSubscription() {
        int expectedNumberOfConsumers = 2;
        Pattern pattern = Pattern.compile(TEST_TOPIC);
        new PatternSubscribableKafkaMessageSourceTestFixture(pattern).testStartBuildsConsumersUpToConsumerCount(expectedNumberOfConsumers);
        verify(mockConsumer, times(expectedNumberOfConsumers)).subscribe(pattern);
    }

    @Test
    void testCloseRunsCloseHandlerPerConsumerCountUsingPatternBasedSubscription() {
        int expectedNumberOfConsumers = 2;
        Pattern pattern = Pattern.compile(TEST_TOPIC);
        new PatternSubscribableKafkaMessageSourceTestFixture(pattern).testCloseRunsCloseHandlerPerConsumerCount(expectedNumberOfConsumers);
        verify(mockConsumer, times(expectedNumberOfConsumers)).subscribe(pattern);
    }

    @Test
    void restartingConsumerShouldNotCauseAMemoryLeakAndOnCloseNoRegistrationsShouldBeleftUsingPatternBasedSubscription() throws NoSuchFieldException, IllegalAccessException {
        Pattern pattern = Pattern.compile(TEST_TOPIC);
        new PatternSubscribableKafkaMessageSourceTestFixture(pattern).restartingConsumerShouldNotCauseAMemoryLeakAndOnCloseNoRegistrationsShouldBeleft();
    }
}