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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.TrackingToken;
import org.junit.*;

import static org.axonframework.extensions.kafka.eventhandling.consumer.KafkaTrackingToken.emptyToken;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link KafkaMessageSource}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public class KafkaMessageSourceTest {

    private Fetcher fetcher = mock(Fetcher.class);

    private KafkaMessageSource testSubject;

    @Before
    public void setUp() {
        testSubject = KafkaMessageSource.builder()
                                        .fetcher(fetcher)
                                        .groupId(DEFAULT_GROUP_ID)
                                        .build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuildingStreamableKafkaMessageSourceMissingRequiredFieldsShouldThrowAxonConfigurationException() {
        KafkaMessageSource.builder().build();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuildingStreamableKafkaMessageSourceUsingInvalidFetcherShouldThrowAxonConfigurationException() {
        KafkaMessageSource.builder().fetcher(null);
    }

    @Test(expected = AxonConfigurationException.class)
    public void testBuildingStreamableKafkaMessageSourceUsingInvalidGroupIdShouldThrowAxonConfigurationException() {
        KafkaMessageSource.builder().groupId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpeningMessageStreamWithInvalidTypeOfTrackingTokenShouldThrowException() {
        testSubject.openStream(incompatibleTokenType());
    }

    @Test
    public void testOpeningMessageStreamWithNullTokenShouldInvokeFetcher() {
        testSubject.openStream(null);

        verify(fetcher, times(1)).start(any(), eq(DEFAULT_GROUP_ID));
    }

    @Test
    public void testOpeningMessageStreamWithValidTokenShouldStartTheFetcher() {
        testSubject.openStream(emptyToken());

        verify(fetcher, times(1)).start(any(), eq(DEFAULT_GROUP_ID));
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
