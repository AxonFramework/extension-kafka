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

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingEventStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Create message stream from a specific Kafka topic. Messages are fetch in bulk and stored in an in-memory buffer. We
 * try to introduce some sort and stored them in a local buffer. Consumer position is tracked via {@link
 * KafkaTrackingToken}. Records are fetched from Kafka and stored in-memory buffer.
 * <p>
 * This is not thread safe.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 * @since 4.0
 */
public class KafkaMessageStream implements TrackingEventStream {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class);

    private final Buffer<KafkaEventMessage> buffer;
    private final Registration closeHandler;
    private KafkaEventMessage peekedEvent;

    /**
     * Create a {@link TrackingEventStream} dedicated to {@link KafkaEventMessage}s. Uses the provided {@code buffer} to
     * retrieve event messages from.
     *
     * @param buffer       the {@link KafkaEventMessage} {@link Buffer} containing the fetched messages
     * @param closeHandler the service {@link Registration} which fills the buffer. Will be canceled upon executing a
     *                     {@link #close()}
     */
    @SuppressWarnings("WeakerAccess")
    public KafkaMessageStream(Buffer<KafkaEventMessage> buffer, Registration closeHandler) {
        assertNonNull(buffer, "Buffer may not be null");
        this.buffer = buffer;
        this.closeHandler = closeHandler;
    }

    @Override
    public Optional<TrackedEventMessage<?>> peek() {
        return Optional.ofNullable(
                peekedEvent == null && !hasNextAvailable(0, TimeUnit.NANOSECONDS) ? null : peekedEvent.value()
        );
    }

    @Override
    public boolean hasNextAvailable(int timeout, TimeUnit unit) {
        try {
            return peekedEvent != null || (peekedEvent = buffer.poll(timeout, unit)) != null;
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public TrackedEventMessage<?> nextAvailable() {
        try {
            return peekedEvent == null ? buffer.take().value() : peekedEvent.value();
        } catch (InterruptedException e) {
            logger.warn("Consumer thread was interrupted. Returning thread to event processor.", e);
            Thread.currentThread().interrupt();
            return null;
        } finally {
            peekedEvent = null;
        }
    }

    @Override
    public void close() {
        if (closeHandler != null) {
            closeHandler.close();
        }
    }
}
