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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.junit.jupiter.api.*;
import org.mockito.*;

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
        when(consumerFactory.createConsumer(GROUP_ID_PREFIX + GROUP_ID_SUFFIX)).thenReturn(mockConsumer);
        fetcher = mock(Fetcher.class);

        testSubject = StreamableKafkaMessageSource.<String, String>builder()
                .groupIdPrefix(GROUP_ID_PREFIX)
                .groupIdSuffixFactory(() -> GROUP_ID_SUFFIX)
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .build();
    }

    @Test
    void testBuildingWithInvalidTopicsShouldThrowAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().topics(null));
    }

    @Test
    void testBuildWithInvalidTopicThrowsAxonConfigurationException() {
        assertThrows(AxonConfigurationException.class, () -> StreamableKafkaMessageSource.builder().addTopic(null));
    }

    @Test
    void testBuildingWithInvalidGroupIdPrefixShouldThrowAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class,
                () -> StreamableKafkaMessageSource.builder().groupIdPrefix(null)
        );
    }

    @Test
    void testBuildingWithInvalidGroupIdSuffixFactoryShouldThrowAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class,
                () -> StreamableKafkaMessageSource.builder().groupIdSuffixFactory(null)
        );
    }

    @Test
    void testBuildingWithInvalidConsumerFactoryShouldThrowAxonConfigurationException() {
        //noinspection unchecked,rawtypes
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
    void testOpeningMessageStreamCreatesGroupIdBasedOnPrefixAndSuffixFactory() {
        String testPrefix = "group-id-prefix";
        String testSuffix = "group-id-suffix";
        when(consumerFactory.createConsumer(testPrefix + testSuffix)).thenReturn(mockConsumer);

        StreamableKafkaMessageSource<String, String> testSubject = StreamableKafkaMessageSource.<String, String>builder()
                .groupIdPrefix(testPrefix)
                .groupIdSuffixFactory(() -> testSuffix)
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .build();

        testSubject.openStream(null).close();

        ArgumentCaptor<String> groupIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(consumerFactory).createConsumer(groupIdCaptor.capture());

        String resultGroupId = groupIdCaptor.getValue();

        assertTrue(resultGroupId.contains(testPrefix));
        assertTrue(resultGroupId.contains(testSuffix));
    }

    @Test
    void testOpeningMessageStreamWithNullTokenShouldInvokeFetcher() {
        AtomicBoolean closed = new AtomicBoolean(false);
        when(fetcher.poll(eq(mockConsumer), any(), any())).thenReturn(() -> {
            closed.set(true);
            return true;
        });

        BlockingStream<TrackedEventMessage<?>> result = testSubject.openStream(null);

        verify(consumerFactory).createConsumer(GROUP_ID_PREFIX + GROUP_ID_SUFFIX);
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

        verify(consumerFactory).createConsumer(GROUP_ID_PREFIX + GROUP_ID_SUFFIX);
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
