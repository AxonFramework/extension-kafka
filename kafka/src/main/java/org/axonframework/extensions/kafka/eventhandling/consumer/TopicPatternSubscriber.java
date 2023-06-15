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

import java.util.regex.Pattern;

/**
 * Implementation of {@link TopicSubscriber} that subscribes a {@link Consumer} to a pattern of topics.
 * Using the {@link Consumer#subscribe(Pattern)} method.
 *
 * @author Ben Kornmeier
 * @since 4.8.0
 */
public class TopicPatternSubscriber implements TopicSubscriber {
    private final Pattern pattern;

    /**
     * Instantiate a {@link TopicPatternSubscriber} that uses {@link Pattern} to subscribe to topics as well as check if it is responsible for a given topic.
     *
     * @param pattern {@link Pattern} to use to subscribe to topics with and check if it is responsible for a given topic.
     */
    public TopicPatternSubscriber(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public void subscribeTopics(Consumer consumer) {
        consumer.subscribe(pattern);
    }

    @Override
    public boolean subscribesToTopicName(String topic) {
        return pattern.matcher(topic).matches();
    }

    @Override
    public String describe() {
        return "pattern=[" + pattern + "]";
    }
}