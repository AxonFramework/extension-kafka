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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Used by {@link org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource.Builder} and {@link org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource.Builder}
 * to provide a {@link KafkaSubscriber} to subscribe a {@link Consumer} to topic(s).
 *
 * @author Ben Kornmeier
 * @since 4.8
 */
public abstract class KafkaBuilderSubscriber<T extends KafkaBuilderSubscriber<T>> {
    protected KafkaSubscriber subscriber = new ListKafkaSubscriber(Collections.singletonList("Axon.Events"));

    /**
     * Allows methods defined in this class to return the concrete class for fluent api usage.
     * @return the current instance of the T
     */
    protected abstract T self();

    public KafkaSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * Set the Kafka {@code topics} to read {@link org.axonframework.eventhandling.EventMessage}s from. Defaults to
     * {@code Axon.Events}.
     *
     * @param topics the Kafka {@code topics} to read {@link org.axonframework.eventhandling.EventMessage}s from
     * @return the current Builder instance, for fluent interfacing
     */
    public T topics(List<String> topics) {
        assertThat(topics, topicList -> Objects.nonNull(topicList) && !topicList.isEmpty(),
                "The topics may not be null or empty");
        this.subscriber = new ListKafkaSubscriber(topics);
        return self();
    }
    /**
     * Add a Kafka {@code topic} to read {@link org.axonframework.eventhandling.EventMessage}s from.
     *
     * @param topic the Kafka {@code topic} to add to the list of topics
     * @return the current Builder instance, for fluent interfacing
     */
    public T addTopic(String topic) {
        assertThat(topic, name -> Objects.nonNull(name) && !"".equals(name), "The topic may not be null or empty");
        if (isListBasedSubscription()) {
            ((ListKafkaSubscriber) subscriber).addTopic(topic);
        } else {
            throw new IllegalStateException("Cannot add topic to a pattern subscriber");
        }
        return self();
    }
    /**
     * Set the Kafka {@code pattern} to read {@link org.axonframework.eventhandling.EventMessage}s from. Defaults to
     * {@code Axon.Events}.
     *
     * @param pattern the Kafka {@code pattern} to read {@link org.axonframework.eventhandling.EventMessage}s from
     * @return the current Builder instance, for fluent interfacing
     */
    public T topicPattern(Pattern pattern) {
        this.subscriber = new PatternKafkaSubscriber(pattern);
        return self();
    }

    /**
     * Return the type of subscription being used.
     * @return if the current subscription is based on a list of topics or a pattern
     */
    private boolean isListBasedSubscription() {
        return subscriber instanceof ListKafkaSubscriber;
    }
}



