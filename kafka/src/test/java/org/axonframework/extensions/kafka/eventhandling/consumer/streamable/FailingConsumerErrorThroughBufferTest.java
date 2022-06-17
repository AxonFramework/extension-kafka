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
package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.FetchEventException;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Kafka tests asserting an error should pop up if there is on error with the consumer. This error pops up via a call to
 * {@link Buffer#setException(RuntimeException)} setException} method in the buffer which pops up once the buffer is
 * empty to the consumer of the {@link StreamableKafkaMessageSource}.
 *
 * @author Gerard Klijs
 */
class FailingConsumerErrorThroughBufferTest {

    private static final String TEST_TOPIC = "failing_consumer_test";
    private Fetcher<String, byte[], KafkaEventMessage> fetcher;
    private ConsumerFactory<String, byte[]> consumerFactory;
    private Consumer<String, byte[]> consumer;

    @BeforeEach
    void setUp() {
        consumer = mock(Consumer.class);
        doThrow(new KafkaException("poll error"))
                .when(consumer)
                .poll(any());
        consumerFactory = new MockFactory();
        fetcher = AsyncFetcher.<String, byte[], KafkaEventMessage>builder()
                              .pollTimeout(300)
                              .build();
    }

    @AfterEach
    void shutdown() {
        fetcher.shutdown();
    }

    @Test
    void testFetchEventExceptionAfterFailedPollWhenCallingNextAvailable() {
        StreamableKafkaMessageSource<String, byte[]> streamableMessageSource =
                StreamableKafkaMessageSource.<String, byte[]>builder()
                                            .topics(Collections.singletonList(TEST_TOPIC))
                                            .consumerFactory(consumerFactory)
                                            .fetcher(fetcher)
                                            .build();
        BlockingStream<TrackedEventMessage<?>> stream = streamableMessageSource.openStream(null);
        assertThrows(FetchEventException.class, stream::nextAvailable);
        stream.close();
    }

    @Test
    void testFetchEventExceptionAfterFailedPollWhenCallingHasNextAvailable() {
        StreamableKafkaMessageSource<String, byte[]> streamableMessageSource =
                StreamableKafkaMessageSource.<String, byte[]>builder()
                                            .topics(Collections.singletonList(TEST_TOPIC))
                                            .consumerFactory(consumerFactory)
                                            .fetcher(fetcher)
                                            .build();
        BlockingStream<TrackedEventMessage<?>> stream = streamableMessageSource.openStream(null);
        AtomicReference<FetchEventException> fetchEventException = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(1L)).until(() -> {
            try {
                stream.hasNextAvailable();
                return false;
            } catch (FetchEventException e) {
                fetchEventException.set(e);
                return true;
            }
        });
        stream.close();
        assertNotNull(fetchEventException.get());
    }

    @Test
    void testFetchEventExceptionAfterFailedPollWhenCallingPeek() {
        StreamableKafkaMessageSource<String, byte[]> streamableMessageSource =
                StreamableKafkaMessageSource.<String, byte[]>builder()
                                            .topics(Collections.singletonList(TEST_TOPIC))
                                            .consumerFactory(consumerFactory)
                                            .fetcher(fetcher)
                                            .build();
        BlockingStream<TrackedEventMessage<?>> stream = streamableMessageSource.openStream(null);
        AtomicReference<FetchEventException> fetchEventException = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(1L)).until(() -> {
            try {
                stream.peek();
                return false;
            } catch (FetchEventException e) {
                fetchEventException.set(e);
                return true;
            }
        });
        stream.close();
        assertNotNull(fetchEventException.get());
    }

    private class MockFactory implements ConsumerFactory<String, byte[]> {

        @Override
        public Consumer<String, byte[]> createConsumer(String groupId) {
            return consumer;
        }
    }
}
