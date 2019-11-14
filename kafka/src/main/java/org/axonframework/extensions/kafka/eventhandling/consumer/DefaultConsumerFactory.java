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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * The {@link ConsumerFactory} implementation to produce a new {@link Consumer} instance. On each invocation of {@link
 * #createConsumer(String)} a new instance will be created based on the supplied {@code configuration} properties.
 *
 * @param <K> the key type of a build {@link Consumer} instance
 * @param <V> the value type of a build {@link Consumer} instance
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class DefaultConsumerFactory<K, V> implements ConsumerFactory<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultConsumerFactory.class);

    private final Map<String, Object> consumerConfiguration;

    /**
     * Build a default {@link ConsumerFactory} which uses the provided {@code configuration} to build it's {@link
     * Consumer}s.
     *
     * @param consumerConfiguration a {@link Map} containing the configuration for the {@link Consumer}s this factory
     *                              builds
     */
    public DefaultConsumerFactory(Map<String, Object> consumerConfiguration) {
        assertNonNull(consumerConfiguration, "The configuration may not be null");
        this.consumerConfiguration = new HashMap<>(consumerConfiguration);
    }

    @Override
    public Consumer<K, V> createConsumer(String groupId) {
        if (this.consumerConfiguration.remove(GROUP_ID_CONFIG) != null) {
            logger.warn("Found a global {} whilst it is required to be provided consciously", GROUP_ID_CONFIG);
        }

        Map<String, Object> consumerConfiguration = new HashMap<>(this.consumerConfiguration);
        consumerConfiguration.put(GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(consumerConfiguration);
    }

    /**
     * Return an unmodifiable reference to the configuration map for this factory. Useful for cloning to make a similar
     * factory.
     *
     * @return a configuration {@link Map} used by this {@link ConsumerFactory} to build {@link Consumer}s
     */
    public Map<String, Object> configurationProperties() {
        return Collections.unmodifiableMap(this.consumerConfiguration);
    }
}
