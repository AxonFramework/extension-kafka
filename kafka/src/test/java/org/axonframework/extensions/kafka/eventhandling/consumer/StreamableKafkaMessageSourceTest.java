/*
 * Copyright (c) 2010-2018. Axon Framework
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
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.emptyToken;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link StreamableKafkaMessageSource}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class StreamableKafkaMessageSourceTest {

    private ConsumerFactory<String, String> consumerFactory;
    private Fetcher<KafkaEventMessage, String, String> fetcher;

    private StreamableKafkaMessageSource testSubject;

    private Consumer<String, String> mockConsumer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        consumerFactory = mock(ConsumerFactory.class);
        mockConsumer = mock(Consumer.class);
        when(consumerFactory.createConsumer(DEFAULT_GROUP_ID)).thenReturn(mockConsumer);
        fetcher = mock(Fetcher.class);

        testSubject = StreamableKafkaMessageSource.<String, String>builder()
                .groupId(DEFAULT_GROUP_ID)
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .build();
    }

    @Test
    void testBuildingWithInvalidTopicShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().topic(null));
    }

    @Test
    void testBuildingWithInvalidGroupIdShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().groupId(null));
    }

    @Test
    void testBuildingWithInvalidConsumerFactoryShouldThrowAxonConfigurationException() {
        //noinspection unchecked
        assertThrows(
                AxonConfigurationException.class,
                () -> StreamableKafkaMessageSource.builder().consumerFactory((ConsumerFactory) null)
        );
    }

    @Test
    void testBuildingWithInvalidFetcherShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().fetcher(null));
    }

    @Test
    void testBuildingWithInvalidMessageConverterShouldThrowAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().messageConverter(null)
        );
    }

    @Test
    void testBuildingWithInvalidBufferFactoryShouldThrowAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().bufferFactory(null)
        );
    }

    @Test
    void testBuildingWhilstMissingRequiredFieldsShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().build());
    }

    @Test
    void testOpeningMessageStreamWithInvalidTypeOfTrackingTokenShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> testSubject.openStream(incompatibleTokenType()));
    }

    @Test
    void testOpeningMessageStreamWithNullTokenShouldInvokeFetcher() {
        AtomicBoolean closed = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> closed.set(true));

        BlockingStream result = testSubject.openStream(null);

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
        verify(fetcher).poll(eq(mockConsumer), any(), any());

        result.close();
        assertTrue(closed.get());
    }

    @Test
    void testOpeningMessageStreamWithValidTokenShouldStartTheFetcher() {
        AtomicBoolean closed = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> closed.set(true));

        BlockingStream result = testSubject.openStream(emptyToken());

        verify(consumerFactory).createConsumer(DEFAULT_GROUP_ID);
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
