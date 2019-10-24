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
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;

/**
 * Test utility for generating a {@link ConsumerConfig}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public abstract class ConsumerConfigUtil {

    /**
     *
     */
    public static final String DEFAULT_GROUP_ID = "groupId";

    private ConsumerConfigUtil() {
        // Utility class
    }

    public static ConsumerFactory<String, Object> transactionalConsumerFactory(EmbeddedKafkaBroker kafkaBroker,
                                                                               String groupName,
                                                                               Class valueDeserializer) {
        return new DefaultConsumerFactory<>(minimalTransactional(kafkaBroker, groupName, valueDeserializer));
    }

    public static ConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker kafkaBroker, String groupName) {
        return new DefaultConsumerFactory<>(minimal(kafkaBroker, groupName));
    }

    public static Map<String, Object> minimalTransactional(EmbeddedKafkaBroker kafkaBroker,
                                                           String groupName,
                                                           Class valueDeserializer) {
        Map<String, Object> configs = minimal(kafkaBroker, groupName, valueDeserializer);
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return configs;
    }

    public static Map<String, Object> minimal(EmbeddedKafkaBroker kafkaBroker, String groupName) {
        return minimal(kafkaBroker, groupName, StringDeserializer.class);
    }

    public static Map<String, Object> minimal(EmbeddedKafkaBroker kafkaBroker,
                                              String groupName,
                                              Class valueDeserializer) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        return config;
    }
}
