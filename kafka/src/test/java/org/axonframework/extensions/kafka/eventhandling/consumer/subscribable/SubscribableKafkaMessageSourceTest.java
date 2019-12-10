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

package org.axonframework.extensions.kafka.eventhandling.consumer.subscribable;

import org.apache.kafka.clients.consumer.Consumer;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
        assertThrows(AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().topics(null));
    }

    @Test
    void testBuildWithInvalidTopicThrowsAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().addTopic(null));
    }

    @Test
    void testBuildWithInvalidGroupIdThrowsAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().groupId(null));
    }

    @Test
    void testBuildWithInvalidConsumerFactoryThrowsAxonConfigurationException() {
        //noinspection unchecked,rawtypes
        assertThrows(
                AxonConfigurationException.class,
                () -> SubscribableKafkaMessageSource.builder().consumerFactory((ConsumerFactory) null)
        );
    }

    @Test
    void testBuildWithInvalidFetcherThrowsAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().fetcher(null));
    }

    @Test
    void testBuildWithInvalidMessageConverterThrowsAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().messageConverter(null)
        );
    }

    @Test
    void testBuildingWhilstMissingRequiredFieldsShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> SubscribableKafkaMessageSource.builder().build());
    }

    @Test
    void testCancelingSubscribedEventProcessorRunsConnectedCloseHandler() {
        AtomicBoolean closedEventProcessor = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> {
            closedEventProcessor.set(true);
            return true;
        });

        Registration registration = testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.start();

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer).subscribe(Collections.singletonList(TEST_TOPIC));

        assertTrue(registration.cancel());
        assertTrue(closedEventProcessor.get());
    }

    @Test
    void testStartOnFirstSubscriptionInitiatesProcessingOnFirstEventProcessor() {
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        SubscribableKafkaMessageSource<String, String> testSubject = SubscribableKafkaMessageSource.<String, String>builder()
                .topics(Collections.singletonList(TEST_TOPIC))
                .groupId(DEFAULT_GROUP_ID)
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .startOnFirstSubscription()
                .build();

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);

        verify(consumerFactory, times(1)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(1)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher).poll(eq(mockConsumer), any(), any());
    }

    @Test
    void testSubscribingTheSameInstanceTwiceDisregardsSecondInstanceOnStart() {
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);

        testSubject.start();

        verify(consumerFactory, times(1)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(1)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher, times(1)).poll(eq(mockConsumer), any(), any());
    }

    @Test
    void testStartSubscribesConsumerToAllProvidedTopics() {
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        List<String> testTopics = new ArrayList<>();
        testTopics.add("topicOne");
        testTopics.add("topicTwo");

        testSubject.subscribe(NO_OP_EVENT_PROCESSOR);
        testSubject.start(testTopics);

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer).subscribe(testTopics);
        verify(fetcher).poll(eq(mockConsumer), any(), any());
    }

    @Test
    void testStartBuildsConsumersForEverySubscribedEventProcessor() {
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(NO_OP_FETCHER_REGISTRATION);

        java.util.function.Consumer<List<? extends EventMessage<?>>> testEventProcessorOne = eventMessages -> {
        };
        testSubject.subscribe(testEventProcessorOne);
        java.util.function.Consumer<List<? extends EventMessage<?>>> testEventProcessorTwo = eventMessages -> {
        };
        testSubject.subscribe(testEventProcessorTwo);

        testSubject.start();

        verify(consumerFactory, times(2)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(2)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher, times(2)).poll(eq(mockConsumer), any(), any());
    }

    @Test
    void testCloseRunsCloseHandlerPerSubscribedEventProcessor() {
        AtomicBoolean closedEventProcessorOne = new AtomicBoolean(false);
        AtomicBoolean closedEventProcessorTwo = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any()))
                .thenReturn(() -> {
                    closedEventProcessorOne.set(true);
                    return true;
                })
                .thenReturn(() -> {
                    closedEventProcessorTwo.set(true);
                    return true;
                });

        java.util.function.Consumer<List<? extends EventMessage<?>>> testEventProcessorOne = eventMessages -> {
        };
        testSubject.subscribe(testEventProcessorOne);
        java.util.function.Consumer<List<? extends EventMessage<?>>> testEventProcessorTwo = eventMessages -> {
        };
        testSubject.subscribe(testEventProcessorTwo);
        testSubject.start();

        testSubject.close();

        verify(consumerFactory, times(2)).createConsumer(DEFAULT_GROUP_ID);
        verify(mockConsumer, times(2)).subscribe(Collections.singletonList(TEST_TOPIC));
        verify(fetcher, times(2)).poll(eq(mockConsumer), any(), any());

        assertTrue(closedEventProcessorOne.get());
        assertTrue(closedEventProcessorTwo.get());
    }
}