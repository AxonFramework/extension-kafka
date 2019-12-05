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

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/**
 * A functional interface towards converting the {@link org.apache.kafka.clients.consumer.ConsumerRecord} instances in
 * to a {@link List} of {@code E}.
 *
 * @param <K> the key of the {@link ConsumerRecords}
 * @param <V> the value type of {@link ConsumerRecords}
 * @param <E> the element type each {@link org.apache.kafka.clients.consumer.ConsumerRecord} instance is converted to
 * @author Steven van Beelen
 * @since 4.0
 */
@FunctionalInterface
public interface RecordConverter<K, V, E> {

    /**
     * Covert the provided {@code records} in to a {@link List} of elements of type {@code E}.
     *
     * @param records a {@link ConsumerRecords} instance to convert in to a {@link List} of {@code E}
     * @return the {@link List} of elements of type {@code E}
     */
    List<E> convert(ConsumerRecords<K, V> records);
}
