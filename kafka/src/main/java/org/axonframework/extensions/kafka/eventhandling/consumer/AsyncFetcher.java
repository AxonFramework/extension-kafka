/*
 * Copyright (c) 2010-2018. Axon Framework
 *
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
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Async implementation of the {@link Fetcher} using an {@link ExecutorService} to schedule {@link FetchEventsTask}s to
 * poll {@link org.apache.kafka.clients.consumer.ConsumerRecords}.
 *
 * @param <E> the element type the {@link org.apache.kafka.clients.consumer.ConsumerRecords} will be converted in to by
 *            the {@link RecordConverter} and consumed by the {@link RecordConsumer}
 * @param <K> the key of the {@link org.apache.kafka.clients.consumer.ConsumerRecords} polled by the {@link
 *            FetchEventsTask}
 * @param <V> the value type of {@link org.apache.kafka.clients.consumer.ConsumerRecords} polled by the {@link
 *            FetchEventsTask}
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class AsyncFetcher<E, K, V> implements Fetcher<E, K, V> {

    private static final int DEFAULT_POLL_TIMEOUT_MS = 5_000;

    private final Duration pollTimeout;
    private final ExecutorService executorService;
    private final boolean requirePoolShutdown;
    private final Set<FetchEventsTask> activeFetchers = ConcurrentHashMap.newKeySet();

    /**
     * Instantiate a Builder to be able to create a {@link AsyncFetcher}.
     * <p>
     * The {@code pollTimeout} is defaulted to a {@link Duration} of {@code 5000} milliseconds and the {@link
     * ExecutorService} to an {@link Executors#newCachedThreadPool()} using an {@link AxonThreadFactory}.
     *
     * @param <K> a generic type for the key of the {@link ConsumerFactory}, {@link ConsumerRecord} and {@link
     *            KafkaMessageConverter}
     * @param <V> a generic type for the value of the {@link ConsumerFactory}, {@link ConsumerRecord} and {@link
     *            KafkaMessageConverter}
     * @return a Builder to be able to create an {@link AsyncFetcher}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Instantiate a {@link AsyncFetcher} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link AsyncFetcher} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected AsyncFetcher(Builder<K, V> builder) {
        this.pollTimeout = builder.pollTimeout;
        this.executorService = builder.executorService;
        this.requirePoolShutdown = builder.requirePoolShutdown;
    }

    @Override
    public Runnable poll(Consumer<K, V> consumer,
                         RecordConverter<E, K, V> recordConverter,
                         RecordConsumer<E> recordConsumer) {
        FetchEventsTask<E, K, V> fetcherTask =
                new FetchEventsTask<>(consumer, pollTimeout, recordConverter, recordConsumer, activeFetchers::remove);

        activeFetchers.add(fetcherTask);
        executorService.execute(fetcherTask);

        return fetcherTask::close;
    }

    @Override
    public void shutdown() {
        activeFetchers.forEach(FetchEventsTask::close);
        if (requirePoolShutdown) {
            executorService.shutdown();
        }
    }

    /**
     * Builder class to instantiate an {@link AsyncFetcher}.
     * <p>
     * The {@code pollTimeout} is defaulted to a {@link Duration} of {@code 5000} milliseconds and the {@link
     * ExecutorService} to an {@link Executors#newCachedThreadPool()} using an {@link AxonThreadFactory}.
     *
     * @param <K> a generic type for the key of the {@link ConsumerFactory}, {@link ConsumerRecord} and {@link
     *            KafkaMessageConverter}
     * @param <V> a generic type for the value of the {@link ConsumerFactory}, {@link ConsumerRecord} and {@link
     *            KafkaMessageConverter}
     */
    public static final class Builder<K, V> {

        private Duration pollTimeout = Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS);
        private ExecutorService executorService = Executors.newCachedThreadPool(new AxonThreadFactory("AsyncFetcher"));
        private boolean requirePoolShutdown = true;

        /**
         * Set the {@code pollTimeout} in milliseconds for polling records from a topic. Defaults to {@code 5000}
         * milliseconds.
         *
         * @param timeoutMillis the timeoutMillis as a {@code long} when reading message from the topic
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> pollTimeout(long timeoutMillis) {
            assertThat(timeoutMillis, timeout -> timeout > 0,
                       "The poll timeout may not be negative [" + timeoutMillis + "]");
            this.pollTimeout = Duration.ofMillis(timeoutMillis);
            return this;
        }

        /**
         * Sets the {@link ExecutorService} used to start {@link FetchEventsTask} instances to poll for Kafka consumer
         * records. Note that the {@code executorService} should contain sufficient threads to run the necessary fetcher
         * processes concurrently. Defaults to an {@link Executors#newCachedThreadPool()} with an {@link
         * AxonThreadFactory}.
         * <p>
         * Note that the provided {@code executorService} will <em>not</em> be shut down when the fetcher is
         * terminated.
         *
         * @param executorService a {@link ExecutorService} used to start {@link FetchEventsTask} instances to poll for
         *                        Kafka consumer records
         * @return the current Builder instance, for fluent interfacing
         */
        @SuppressWarnings("WeakerAccess")
        public Builder<K, V> executorService(ExecutorService executorService) {
            assertNonNull(executorService, "ExecutorService may not be null");
            this.requirePoolShutdown = false;
            this.executorService = executorService;
            return this;
        }

        /**
         * Initializes a {@link AsyncFetcher} as specified through this Builder.
         *
         * @return a {@link AsyncFetcher} as specified through this Builder
         */
        public AsyncFetcher build() {
            return new AsyncFetcher<>(this);
        }
    }
}
