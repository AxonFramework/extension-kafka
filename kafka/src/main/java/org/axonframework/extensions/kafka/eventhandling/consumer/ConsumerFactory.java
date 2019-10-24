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

/**
 * A functional interface towards building {@link Consumer} instances.
 *
 * @param <K> the key type of a build {@link Consumer} instance
 * @param <V> the value type of a build {@link Consumer} instance
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
@FunctionalInterface
public interface ConsumerFactory<K, V> {

    /**
     * Create a {@link Consumer} that should be part of the Consumer Group with the given {@code groupId}.
     *
     * @return a {@link Consumer} which is part of Consumer Group with the given {@code groupId}
     */
    Consumer<K, V> createConsumer(String groupId);
}
