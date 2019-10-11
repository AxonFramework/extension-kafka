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

import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.messaging.StreamableMessageSource;

import static org.axonframework.common.Assert.isTrue;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Implementation of the {@link StreamableMessageSource} that reads messages from a Kafka topic using the provided
 * {@link Fetcher}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaMessageSource implements StreamableMessageSource<TrackedEventMessage<?>> {

    private final Fetcher fetcher;

    /**
     * Initialize the source using the given {@code fetcher} to retrieve messages from a Kafka topic.
     *
     * @param fetcher the {@link Fetcher} used to retrieve messages from a Kafka topic
     */
    public KafkaMessageSource(Fetcher fetcher) {
        assertNonNull(fetcher, "Kafka message fetcher may not be null");
        this.fetcher = fetcher;
    }

    @SuppressWarnings("ConstantConditions") // Verified TrackingToken type through `Assert.isTrue` operation
    @Override
    public BlockingStream<TrackedEventMessage<?>> openStream(TrackingToken trackingToken) {
        isTrue(
                trackingToken == null || trackingToken instanceof KafkaTrackingToken,
                () -> "Incompatible token type provided."
        );
        return fetcher.start((KafkaTrackingToken) trackingToken);
    }
}
