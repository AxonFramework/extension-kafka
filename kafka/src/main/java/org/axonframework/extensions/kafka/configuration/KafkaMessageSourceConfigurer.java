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

package org.axonframework.extensions.kafka.configuration;

import org.axonframework.config.Component;
import org.axonframework.config.Configuration;
import org.axonframework.config.ModuleConfiguration;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link ModuleConfiguration} to configure Kafka as a message source for {@link
 * org.axonframework.eventhandling.EventProcessor} instances. This ModuleConfiguration should be registered towards the
 * {@link org.axonframework.config.Configurer} for it to start amy Kafka sources for an EventProcessor.
 *
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaMessageSourceConfigurer implements ModuleConfiguration {

    private Configuration configuration;
    private final List<Component<SubscribableKafkaMessageSource<?, ?>>> subscribableKafkaMessageSources = new ArrayList<>();

    @Override
    public void initialize(Configuration config) {
        this.configuration = config;
    }

    @Override
    public int phase() {
        return Integer.MAX_VALUE;
    }

    /**
     * Register a {@link Function} which uses the provided {@link Configuration} to build a {@link
     * SubscribableKafkaMessageSource}.
     *
     * @param subscribableKafkaMessageSource the {@link Function} which will build a {@link SubscribableKafkaMessageSource}
     */
    public void registerSubscribableSource(
            Function<Configuration, SubscribableKafkaMessageSource<?, ?>> subscribableKafkaMessageSource) {
        subscribableKafkaMessageSources.add(new Component<>(
                () -> configuration, "subscribableKafkaMessageSource", subscribableKafkaMessageSource
        ));
    }

    @Override
    public void start() {
        subscribableKafkaMessageSources.stream().map(Component::get).forEach(SubscribableKafkaMessageSource::start);
    }

    @Override
    public void shutdown() {
        subscribableKafkaMessageSources.stream().map(Component::get).forEach(SubscribableKafkaMessageSource::close);
    }
}
