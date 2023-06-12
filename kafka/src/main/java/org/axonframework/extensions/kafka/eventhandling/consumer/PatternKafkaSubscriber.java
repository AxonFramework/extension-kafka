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
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
/**
 * Implementation of {@link KafkaSubscriber} that subscribes a {@link Consumer} a pattern of topics.
 * Using the {@link Consumer#subscribe(Pattern)} method.
 *
 * @author Ben Kornmeier
 * @since 4.8
 */
public class PatternKafkaSubscriber implements KafkaSubscriber {
    private Pattern pattern = Pattern.compile("Axon.Events");

    public PatternKafkaSubscriber(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public void subscribeTopics(Consumer consumer) {
        consumer.subscribe(pattern);
    }
}
