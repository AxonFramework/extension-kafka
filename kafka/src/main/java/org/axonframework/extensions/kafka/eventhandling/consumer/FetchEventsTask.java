/*
 * Copyright (c) 2010-2018. Axon Framework
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.axonframework.common.Assert.nonNull;
import static org.axonframework.common.BuilderUtils.assertThat;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Polls a {@link Consumer} for new records and inserts these, after conversion by the {@link KafkaMessageConverter}, in
 * a {@link Buffer}. It is also in charge of updating the {@link KafkaTrackingToken} it's partitions and their
 * respective offsets.
 *
 * @param <K> the key of the Kafka {@link ConsumerRecords}
 * @param <V> the value type of Kafka {@link ConsumerRecords}
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
class FetchEventsTask<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FetchEventsTask.class);

    private final Consumer<K, V> consumer;
    private final Duration pollTimeout;
    private final KafkaMessageConverter<K, V> converter;
    private final Buffer<KafkaEventMessage> buffer;
    private final java.util.function.Consumer<FetchEventsTask> closeHandler;

    private KafkaTrackingToken currentToken;
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Create a fetch events {@link Runnable} task. The {@link Consumer} is used to periodically {@link
     * Consumer#poll(Duration)} for new {@link ConsumerRecords}. These are in turn converted (with the given {@code
     * converter}) and appended to the {@code buffer}. Every new {@link ConsumerRecord} will advance the given {@code
     * token}'s position.
     *
     * @param consumer     the {@link Consumer} used to {@link Consumer#poll(Duration)} {@link ConsumerRecords} from
     * @param pollTimeout  the {@link Duration} used for the {@link Consumer#poll(Duration)} call
     * @param converter    the {@link KafkaMessageConverter} used to convert a {@link ConsumerRecord} in to a {@link
     *                     KafkaEventMessage}
     * @param buffer       the {@link Buffer} used to store fetched {@link KafkaEventMessage}s in
     * @param closeHandler the handler called after this {@link Runnable} is shutdown
     * @param token        the {@link KafkaTrackingToken} to advance for every fetched {@link ConsumerRecord}
     */
    FetchEventsTask(Consumer<K, V> consumer,
                    Duration pollTimeout,
                    KafkaMessageConverter<K, V> converter,
                    Buffer<KafkaEventMessage> buffer,
                    java.util.function.Consumer<FetchEventsTask> closeHandler,
                    KafkaTrackingToken token) {
        this.consumer = nonNull(consumer, () -> "Consumer may not be null");
        assertThat(
                pollTimeout, time -> !time.isNegative(),
                "The poll timeout may not be negative [" + pollTimeout + "]"
        );
        this.pollTimeout = pollTimeout;
        this.converter = nonNull(converter, () -> "Converter may not be null");
        this.buffer = nonNull(buffer, () -> "Buffer may not be null");
        this.closeHandler = getOrDefault(closeHandler, task -> { /* no-op */ });
        this.currentToken = nonNull(token, () -> "Token may not be null");
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                Collection<KafkaEventMessage> eventMessages = new ArrayList<>(records.count());
                logger.debug("Fetched [{}] number of records", records.count());

                // Convert every ConsumerRecord into a KafkaEventMessage and advance the TrackingToken
                for (ConsumerRecord<K, V> record : records) {
                    converter.readKafkaMessage(record).ifPresent(eventMessage -> {
                        KafkaTrackingToken nextToken = currentToken.advancedTo(record.partition(), record.offset());
                        logger.debug("Advancing token from [{}] to [{}]", currentToken, nextToken);
                        currentToken = nextToken;

                        eventMessages.add(KafkaEventMessage.from(eventMessage, record, currentToken));
                    });
                }

                // Add all the event messages in the buffer
                try {
                    buffer.putAll(eventMessages);
                } catch (InterruptedException e) {
                    logger.debug("Event Consumer thread was interrupted. Shutting down", e);
                    running.set(false);
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            logger.error("Cannot proceed with fetching ConsumerRecords since we encountered an exception", e);
        } finally {
            running.set(false);
            closeHandler.accept(this);
            consumer.close();
        }
    }

    /**
     * Shutdown this task.
     */
    public void close() {
        this.running.set(false);
    }
}
