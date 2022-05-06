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
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken.emptyToken;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link StreamableKafkaMessageSource}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @author Gerard Klijs
 */
class StreamableKafkaMessageSourceTest {

    private static final String GROUP_ID_PREFIX = DEFAULT_GROUP_ID + "-";
    private static final String GROUP_ID_SUFFIX = "WithSuffix";

    private ConsumerFactory<String, String> consumerFactory;
    private Fetcher<String, String, KafkaEventMessage> fetcher;

    private StreamableKafkaMessageSource<String, String> testSubject;

    private Consumer<String, String> mockConsumer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        consumerFactory = mock(ConsumerFactory.class);
        mockConsumer = mock(Consumer.class);
        when(consumerFactory.createConsumer(null)).thenReturn(mockConsumer);
        fetcher = mock(Fetcher.class);

        testSubject = StreamableKafkaMessageSource.<String, String>builder()
                                                  .consumerFactory(consumerFactory)
                                                  .fetcher(fetcher)
                                                  .build();
    }

    @Test
    void testBuildingWithInvalidTopicsShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.topics(null));
    }

    @Test
    void testBuildWithInvalidTopicThrowsAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.addTopic(null));
    }

    @Test
    void testBuildingWithInvalidGroupIdPrefixShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(
                AxonConfigurationException.class,
                () -> builder.groupIdPrefix(null)
        );
    }

    @Test
    void testBuildingWithInvalidGroupIdSuffixFactoryShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(
                AxonConfigurationException.class,
                () -> builder.groupIdSuffixFactory(null)
        );
    }

    @Test
    void testBuildingWithInvalidConsumerFactoryShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        //noinspection unchecked,rawtypes
        assertThrows(
                AxonConfigurationException.class,
                () -> builder.consumerFactory((ConsumerFactory) null)
        );
    }

    @Test
    void testBuildingWithInvalidFetcherShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.fetcher(null));
    }

    @Test
    void testBuildingWithInvalidMessageConverterShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(
                AxonConfigurationException.class, () -> builder.messageConverter(null)
        );
    }

    @Test
    void testBuildingWithInvalidBufferFactoryShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(
                AxonConfigurationException.class, () -> builder.bufferFactory(null)
        );
    }

    @Test
    void testBuildingWhilstMissingRequiredFieldsShouldThrowAxonConfigurationException() {
        StreamableKafkaMessageSource.Builder<Object, Object> builder = StreamableKafkaMessageSource.builder();
        assertThrows(AxonConfigurationException.class, builder::build);
    }

    @Test
    void testOpeningMessageStreamWithInvalidTypeOfTrackingTokenShouldThrowException() {
        TrackingToken incompatibleTokenType = incompatibleTokenType();
        assertThrows(IllegalArgumentException.class, () -> testSubject.openStream(incompatibleTokenType));
    }

    @Test
    void testOpeningMessageStreamWithNullTokenShouldInvokeFetcher() {
        AtomicBoolean closed = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> {
            closed.set(true);
            return true;
        });

        BlockingStream<TrackedEventMessage<?>> result = testSubject.openStream(null);

        verify(consumerFactory).createConsumer(null);
        verify(fetcher).poll(eq(mockConsumer), any(), any());

        result.close();
        assertTrue(closed.get());
    }

    @Test
    void testOpeningMessageStreamWithValidTokenShouldStartTheFetcher() {
        AtomicBoolean closed = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> {
            closed.set(true);
            return true;
        });

        BlockingStream<TrackedEventMessage<?>> result = testSubject.openStream(emptyToken());

        verify(consumerFactory).createConsumer(null);
        verify(fetcher).poll(eq(mockConsumer), any(), any());

        result.close();
        assertTrue(closed.get());
    }

    private static TrackingToken incompatibleTokenType() {
        return new TrackingToken() {
            @Override
            public TrackingToken lowerBound(TrackingToken other) {
                return null;
            }

            @Override
            public TrackingToken upperBound(TrackingToken other) {
                return null;
            }

            @Override
            public boolean covers(TrackingToken other) {
                return false;
            }
        };
    }
}
