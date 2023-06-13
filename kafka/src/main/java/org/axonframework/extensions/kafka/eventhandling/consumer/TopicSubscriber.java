/*
 * Copyright (c) 2010-2023. Axon Framework
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

/**
 * Used by {@link org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource} and {@link org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource} to subscribe a {@link Consumer}
 * to topic(s).
 *
 * @author Ben Kornmeier
 * @since 4.8.0
 */
public interface TopicSubscriber {
    /**
     * Subscribes the given {@link Consumer} to the topic(s) this {@link TopicSubscriber} is responsible for.
     *
     * @param consumer
     */
    void subscribeTopics(Consumer consumer);

    /**
     * Checks if this {@link TopicSubscriber} is responsible for the given topic.
     *
     * @param topic
     * @return true if this {@link TopicSubscriber} is responsible for the given topic, false otherwise.
     */
    boolean subscribesToTopicName(String topic);

    /**
     * Returns a description of the topic(s) this {@link TopicSubscriber} is responsible for.
     * @return
     */
    String describe();
}