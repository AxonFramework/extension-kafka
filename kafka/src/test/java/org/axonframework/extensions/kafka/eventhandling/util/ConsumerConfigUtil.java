/*
 * Copyright (c) 2010-2018. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Test utility for generating a {@link org.apache.kafka.clients.consumer.Consumer} configuration map to be used in
 * tests or an entire {@link ConsumerFactory} at once.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public abstract class ConsumerConfigUtil {

    /**
     * A default Consumer Group group id used for testing.
     */
    public static final String DEFAULT_GROUP_ID = "groupId";

    private ConsumerConfigUtil() {
        // Utility class
    }

    /**
     * Build a minimal, transactional {@link ConsumerFactory} to be used during testing only.
     *
     * @param bootstrapServer   the Kafka Container address
     * @param valueDeserializer a {@link Class} defining the type of value deserializer to be used
     * @return a {@link ConsumerFactory} configured with the minimal properties based on the given {@code
     * kafkaContainer} and {@code valueDeserializer}
     */
    public static ConsumerFactory<String, Object> transactionalConsumerFactory(String bootstrapServer,
                                                                               Class valueDeserializer) {
        return new DefaultConsumerFactory<>(minimalTransactional(bootstrapServer, valueDeserializer));
    }

    /**
     * Build a minimal, transactional, {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     *
     * @param bootstrapServer   the Kafka Container address
     * @param valueDeserializer a {@link Class} defining the type of value deserializer to be used
     * @return a minimal, transactional, {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     */
    @SuppressWarnings("WeakerAccess")
    public static Map<String, Object> minimalTransactional(String bootstrapServer, Class valueDeserializer) {
        Map<String, Object> configs = minimal(bootstrapServer, valueDeserializer);
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return configs;
    }

    /**
     * Build a minimal {@link ConsumerFactory} to be used during testing only.
     *
     * @param bootstrapServer the Kafka Container address
     * @return a {@link ConsumerFactory} configured with the minimal properties based on the given {@code
     * kafkaContainer}
     */
    public static ConsumerFactory<String, String> consumerFactory(String bootstrapServer) {
        return new DefaultConsumerFactory<>(minimal(bootstrapServer));
    }

    /**
     * Build a minimal {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     *
     * @param bootstrapServer the Kafka Container address
     * @return a minimal {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     */
    public static Map<String, Object> minimal(String bootstrapServer) {
        return minimal(bootstrapServer, StringDeserializer.class);
    }

    /**
     * Build a minimal {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     *
     * @param bootstrapServer   the Kafka Container address
     * @param valueDeserializer a {@link Class} defining the type of value deserializer to be used
     * @return a minimal {@link org.apache.kafka.clients.consumer.Consumer} configuration {@link Map}
     */
    public static Map<String, Object> minimal(String bootstrapServer, Class valueDeserializer) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return config;
    }
}
