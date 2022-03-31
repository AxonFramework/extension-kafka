/*
 * Copyright (c) 2010-2022. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.eventhandling.EventMessage;

import java.util.Optional;
import java.util.function.Function;

/**
 * Interface to determine if an {@code EventMessage} should be published to Kafka, and if so to which topic. If the
 * result from the call is {@code Optional.empty()} is will not be published, else the result will be used for the
 * topic.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
@FunctionalInterface
public interface TopicResolver extends Function<EventMessage<?>, Optional<String>> {

    /**
     * resolve an {@code EventMessage} to an optional topic to publish the event to
     *
     * @param event an {@code EventMessage}
     * @return the optional topic, when empty the event message will not be published
     */
    default Optional<String> resolve(EventMessage<?> event) {
        return this.apply(event);
    }
}
