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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.axonframework.common.Assert.nonNull;
import static org.axonframework.common.BuilderUtils.assertThat;
import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * Polls {@link ConsumerRecords} with a {@link Consumer}. These ConsumerRecords will be converted by a {@link
 * RecordConverter} and afterwards consumed by a {@link EventConsumer}.
 *
 * @param <K> the key of the Kafka {@link ConsumerRecords} to be polled, converted and consumed
 * @param <V> the value type of Kafka {@link ConsumerRecords} to be polled, converted and consumed
 * @param <E> the element type each {@link org.apache.kafka.clients.consumer.ConsumerRecord} instance is converted to
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
class FetchEventsTask<K, V, E> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Consumer<K, V> consumer;
    private final Duration pollTimeout;
    private final RecordConverter<K, V, E> recordConverter;
    private final EventConsumer<E> eventConsumer;
    private final java.util.function.Consumer<FetchEventsTask<K, V, E>> closeHandler;

    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Create a fetch events {@link Runnable} task. The {@link Consumer} is used to periodically {@link
     * Consumer#poll(Duration)} for new {@link ConsumerRecords}. These are in turn converted with the given {@code
     * recordConverter} and consumed by the {@code recordConsumer}.
     *
     * @param consumer        the {@link Consumer} used to {@link Consumer#poll(Duration)} {@link ConsumerRecords} from
     * @param pollTimeout     the {@link Duration} used for the {@link Consumer#poll(Duration)} call
     * @param recordConverter the {@link RecordConverter} used to convert the retrieved {@link ConsumerRecords}
     * @param eventConsumer   the {@link EventConsumer} used to consume the converted {@link ConsumerRecords}
     * @param closeHandler    the handler called after this {@link Runnable} is shutdown
     */
    FetchEventsTask(Consumer<K, V> consumer,
                    Duration pollTimeout,
                    RecordConverter<K, V, E> recordConverter,
                    EventConsumer<E> eventConsumer,
                    java.util.function.Consumer<FetchEventsTask<K, V, E>> closeHandler) {
        this.consumer = nonNull(consumer, () -> "Consumer may not be null");
        assertThat(pollTimeout, time -> !time.isNegative(),
                   "The poll timeout may not be negative [" + pollTimeout + "]");
        this.pollTimeout = pollTimeout;
        this.recordConverter = recordConverter;
        this.eventConsumer = eventConsumer;
        this.closeHandler = getOrDefault(closeHandler, task -> { /* no-op */ });
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                logger.debug("Fetched [{}] number of ConsumerRecords", records.count());
                List<E> convertedMessages = recordConverter.convert(records);
                try {
                    if (!convertedMessages.isEmpty()) {
                        eventConsumer.consume(convertedMessages);
                    }
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
            logger.info("Fetch events task and used Consumer instance [{}] have been closed", consumer);
        }
    }

    /**
     * Shutdown this {@link FetchEventsTask}.
     */
    public void close() {
        logger.info("Closing down FetchEventsTask using Consumer [{}]", consumer);
        this.running.set(false);
    }
}
