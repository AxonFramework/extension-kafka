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

package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode.WAIT_FOR_ACK;

/**
 * Test utility for generating a {@link ProducerConfig}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
public abstract class ProducerConfigUtil {

    private ProducerConfigUtil() {
        // Utility class
    }

    /**
     * Empty configuration for a {@link KafkaProducer}. We can't publish anything using it.
     *
     * @return the configuration.
     */
    public static Map<String, Object> empty() {
        return Collections.emptyMap();
    }

    /**
     * Minimal configuration required for creating a {@link KafkaProducer}.
     * <ul>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker the {@link EmbeddedKafkaBroker} being used for testing
     * @return the configuration.
     */
    public static Map<String, Object> minimal(EmbeddedKafkaBroker kafkaBroker) {
        return minimal(kafkaBroker, StringSerializer.class);
    }

    /**
     * Minimal configuration required for creating a {@link KafkaProducer}.
     * <ul>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker     the {@link EmbeddedKafkaBroker} being used for testing
     * @param valueSerializer the serializer for <code>value</code> that implements {@link Serializer}.
     * @return the configuration.
     */
    public static Map<String, Object> minimal(EmbeddedKafkaBroker kafkaBroker, Class valueSerializer) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        configs.put(ProducerConfig.RETRIES_CONFIG, 0);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return configs;
    }

    /**
     * Minimal configuration required for creating a transactional {@link KafkaProducer}.
     * <ul>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker the {@link EmbeddedKafkaBroker} being used for testing
     * @return the configuration.
     */
    public static Map<String, Object> minimalTransactional(EmbeddedKafkaBroker kafkaBroker) {
        return minimalTransactional(kafkaBroker, StringSerializer.class);
    }

    /**
     * Minimal configuration required for creating a transactional {@link KafkaProducer}.
     * <ul>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker     the {@link EmbeddedKafkaBroker} being used for testing
     * @param valueSerializer the serializer for <code>value</code> that implements {@link Serializer}.
     * @return the configuration.
     */
    public static Map<String, Object> minimalTransactional(EmbeddedKafkaBroker kafkaBroker, Class valueSerializer) {
        Map<String, Object> configs = minimal(kafkaBroker, valueSerializer);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.RETRIES_CONFIG, 1);
        return configs;
    }

    /**
     * Factory for generating {@link KafkaProducer} with:
     * <ul>
     * <li><code>confirmationMode</code> - {@link ConfirmationMode#NONE}.</li>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker the Kafka.
     * @return the producer factory.
     */
    public static ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker kafkaBroker) {
        return DefaultProducerFactory.<String, String>builder()
                .configuration(minimal(kafkaBroker))
                .closeTimeout(100, ChronoUnit.MILLIS)
                .build();
    }

    /**
     * Factory for generating {@link KafkaProducer} with:
     * <ul>
     * <li><code>confirmationMode</code> - {@link ConfirmationMode#WAIT_FOR_ACK}.</li>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker     the {@link EmbeddedKafkaBroker} being used for testing
     * @param valueSerializer The serializer for <code>value</code> that implements {@link Serializer}.
     * @return the producer factory.
     */
    public static <V> ProducerFactory<String, V> ackProducerFactory(EmbeddedKafkaBroker kafkaBroker,
                                                                    Class valueSerializer) {
        return DefaultProducerFactory.<String, V>builder()
                .closeTimeout(1000, ChronoUnit.MILLIS)
                .configuration(minimal(kafkaBroker, valueSerializer))
                .confirmationMode(WAIT_FOR_ACK)
                .build();
    }

    /**
     * Factory for generating transactional {@link KafkaProducer} with:
     * <ul>
     * <li><code>confirmationMode</code> - {@link ConfirmationMode#TRANSACTIONAL}.</li>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker           the {@link EmbeddedKafkaBroker} being used for testing
     * @param transactionalIdPrefix prefix for generating <code>transactional.id</code>.
     * @return the producer factory.
     */
    public static ProducerFactory<String, String> transactionalProducerFactory(EmbeddedKafkaBroker kafkaBroker,
                                                                               String transactionalIdPrefix) {
        return DefaultProducerFactory.<String, String>builder()
                .closeTimeout(100, ChronoUnit.MILLIS)
                .configuration(minimalTransactional(kafkaBroker))
                .transactionalIdPrefix(transactionalIdPrefix)
                .build();
    }

    /**
     * Factory for generating transactional {@link KafkaProducer} with:
     * <ul>
     * <li><code>confirmationMode</code> - {@link ConfirmationMode#TRANSACTIONAL}.</li>
     * <li><code>key.serializer</code> - {@link org.apache.kafka.common.serialization.StringSerializer}.</li>
     * </ul>
     *
     * @param kafkaBroker           the {@link EmbeddedKafkaBroker} being used for testing
     * @param transactionalIdPrefix prefix for generating <code>transactional.id</code>.
     * @param valueSerializer       The serializer for <code>value</code> that implements {@link Serializer}.
     * @return the producer factory.
     */
    public static <V> ProducerFactory<String, V> transactionalProducerFactory(EmbeddedKafkaBroker kafkaBroker,
                                                                              String transactionalIdPrefix,
                                                                              Class valueSerializer) {
        return DefaultProducerFactory.<String, V>builder()
                .closeTimeout(100, ChronoUnit.MILLIS)
                .configuration(minimalTransactional(kafkaBroker, valueSerializer))
                .transactionalIdPrefix(transactionalIdPrefix)
                .build();
    }
}
