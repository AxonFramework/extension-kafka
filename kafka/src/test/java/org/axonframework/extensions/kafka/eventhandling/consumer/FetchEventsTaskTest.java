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

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaEventMessage;
import org.junit.jupiter.api.*;
import org.mockito.verification.*;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.axonframework.extensions.kafka.eventhandling.util.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link FetchEventsTask}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class FetchEventsTaskTest {

    private static final int TIMEOUT_MILLIS = 100;

    private ConsumerRecords<String, String> consumerRecords;
    private KafkaEventMessage kafkaEventMessage;
    private final AtomicBoolean expectedToBeClosed = new AtomicBoolean(false);

    private Consumer<String, String> testConsumer;
    private Duration testPollTimeout;
    private RecordConverter<String, String, KafkaEventMessage> testRecordConverter;
    private EventConsumer<KafkaEventMessage> testEventConsumer;
    private java.util.function.Consumer<FetchEventsTask<String, String, KafkaEventMessage>> testCloseHandler;
    private FetchEventsTask<String, String, KafkaEventMessage> testSubject;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        consumerRecords = mock(ConsumerRecords.class);
        kafkaEventMessage = mock(KafkaEventMessage.class);

        testConsumer = mock(KafkaConsumer.class);
        testPollTimeout = Duration.ofMillis(10);
        testRecordConverter = mock(RecordConverter.class);
        testEventConsumer = mock(EventConsumer.class);
        testCloseHandler = task -> expectedToBeClosed.set(true);

        testSubject = new FetchEventsTask<>(
                testConsumer, testPollTimeout, testRecordConverter, testEventConsumer, testCloseHandler
        );

        when(testConsumer.poll(testPollTimeout)).thenReturn(consumerRecords);
        when(testRecordConverter.convert(consumerRecords)).thenReturn(Collections.singletonList(kafkaEventMessage));
    }

    @SuppressWarnings({"ConstantConditions"})
    @Test
    void testTaskConstructionWithInvalidConsumerShouldThrowException() {
        Consumer<String, String> invalidConsumer = null;
        assertThrows(
                IllegalArgumentException.class,
                () -> new FetchEventsTask<>(
                        invalidConsumer, testPollTimeout, testRecordConverter, testEventConsumer, testCloseHandler
                )
        );
    }

    @Test
    void testTaskConstructionWithNegativeTimeoutShouldThrowException() {
        Duration negativeTimeout = Duration.ofMillis(-1);
        assertThrows(
                AxonConfigurationException.class,
                () -> new FetchEventsTask<>(
                        testConsumer, negativeTimeout, testRecordConverter, testEventConsumer, testCloseHandler
                )
        );
    }

    @Test
    void testFetchEventsTaskInterruptionClosesAsExpected() {
        EventConsumer<KafkaEventMessage> failingEventConsumer = events -> {
            throw new InterruptedException();
        };
        FetchEventsTask<String, String, KafkaEventMessage> testSubjectWithFailingEventConsumer = new FetchEventsTask<>(
                testConsumer, testPollTimeout, testRecordConverter, failingEventConsumer, testCloseHandler
        );

        Thread taskRunner = new Thread(testSubjectWithFailingEventConsumer);
        taskRunner.start();


        assertWithin(Duration.ofMillis(TIMEOUT_MILLIS), () -> assertTrue(expectedToBeClosed.get()));
        verify(testConsumer).close();
    }

    @Test
    void testFetchEventsTaskPollsConvertsAndConsumesRecords() throws InterruptedException {
        VerificationMode atLeastOnceWithTimeout = timeout(TIMEOUT_MILLIS).atLeastOnce();

        Thread taskRunner = new Thread(testSubject);
        taskRunner.start();

        verify(testConsumer, atLeastOnceWithTimeout).poll(testPollTimeout);
        verify(testRecordConverter, atLeastOnceWithTimeout).convert(consumerRecords);
        verify(testEventConsumer, atLeastOnceWithTimeout).consume(Collections.singletonList(kafkaEventMessage));

        taskRunner.interrupt();
    }

    @Test
    void testFetchEventsTaskPollsDoesNotCallEventConsumerForZeroConvertedEvents() {
        VerificationMode atLeastOnceWithTimeout = timeout(TIMEOUT_MILLIS).atLeastOnce();
        when(testRecordConverter.convert(any())).thenReturn(Collections.emptyList());

        Thread taskRunner = new Thread(testSubject);
        taskRunner.start();

        verify(testConsumer, atLeastOnceWithTimeout).poll(testPollTimeout);
        verify(testRecordConverter, atLeastOnceWithTimeout).convert(consumerRecords);
        verifyNoMoreInteractions(testEventConsumer);

        taskRunner.interrupt();
    }

    @Test
    void testCloseCallsProvidedCloseHandler() {
        Thread taskRunner = new Thread(testSubject);
        taskRunner.start();

        testSubject.close();

        assertWithin(Duration.ofMillis(TIMEOUT_MILLIS), () -> assertTrue(expectedToBeClosed.get()));
        verify(testConsumer, timeout(TIMEOUT_MILLIS)).close();
    }
}
