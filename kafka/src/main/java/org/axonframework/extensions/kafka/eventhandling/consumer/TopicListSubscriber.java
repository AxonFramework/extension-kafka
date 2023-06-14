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

import java.util.Collection;

/**
 * Implementation of {@link TopicSubscriber} that subscribes a {@link Consumer} to a list of topics.
 * Using the {@link Consumer#subscribe(Collection)} method. This was standard behavior prior to 4.8.0
 *
 * @author Ben Kornmeier
 * @since 4.8.0
 */
public class TopicListSubscriber implements TopicSubscriber {
    private final Collection<String> topics;

    /**
     * Instantiate a {@link TopicListSubscriber} that is backed by a list of specific topics.
     *
     * @param topics the topics to subscribe to
     */
    public TopicListSubscriber(Collection<String> topics) {
        this.topics = topics;
    }

    /**
     * Adds a topic to the list of topics.
     *
     * @param topic the topic to add
     */
    public void addTopic(String topic) {
        if(topic != null && !topic.isEmpty())
            this.topics.add(topic);
    }

    @Override
    public void subscribeTopics(Consumer consumer) {
        consumer.subscribe(topics);
    }

    @Override
    public boolean subscribesToTopicName(String topic) {
        return topics.contains(topic);
    }

    @Override
    public String describe() {
        return "topics=[" + String.join(", ", topics) + "]";
    }
}