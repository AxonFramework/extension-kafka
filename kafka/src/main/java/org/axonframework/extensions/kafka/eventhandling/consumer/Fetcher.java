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
import org.axonframework.common.Registration;

/**
 * Interface describing the component responsible for fetching messages from a Kafka topic through a {@link Consumer}.
 *
 * @param <K> the key of the {@link org.apache.kafka.clients.consumer.ConsumerRecords} produced in the {@link Consumer}
 *            and used in the {@link RecordConverter}
 * @param <V> the value type of {@link org.apache.kafka.clients.consumer.ConsumerRecords} produced in the {@link
 *            Consumer} and used in the {@link RecordConverter}
 * @param <E> the element type the {@link org.apache.kafka.clients.consumer.ConsumerRecords} will be converted in to by
 *            the {@link RecordConverter} and consumed by the {@link EventConsumer}
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public interface Fetcher<K, V, E> {

    /**
     * Instruct this Fetcher to start polling message through the provided {@link Consumer}. After retrieval, the {@link
     * org.apache.kafka.clients.consumer.ConsumerRecords} will be converted by the given {@code recordConverter} and
     * there after consumed by the given {@code recordConsumer}. A {@link Registration} will be returned to cancel
     * message consumption and conversion.
     *
     * @param consumer        the {@link Consumer} used to consume message from a Kafka topic
     * @param recordConverter a {@link RecordConverter} instance which will convert the "consumed" {@link
     *                        org.apache.kafka.clients.consumer.ConsumerRecords} in to a  List of {@code E}
     * @param eventConsumer   a {@link EventConsumer} instance which will consume the converted records
     * @return a close handler of type {@link org.axonframework.common.Registration} to stop the {@link Fetcher}
     * operation
     */
    Registration poll(Consumer<K, V> consumer,
                      RecordConverter<K, V, E> recordConverter,
                      EventConsumer<E> eventConsumer);

    /**
     * Shuts the fetcher down, closing any resources used by this fetcher.
     */
    void shutdown();
}
