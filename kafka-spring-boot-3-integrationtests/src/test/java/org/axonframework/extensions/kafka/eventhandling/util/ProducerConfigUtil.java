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

package org.axonframework.extensions.kafka.eventhandling.util;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

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
     * Minimal configuration required for creating a {@link KafkaProducer}.
     * <ul>
     * <li><code>key.serializer</code> - {@link StringSerializer}.</li>
     * <li><code>value.serializer</code> - {@link StringSerializer}.</li>
     * </ul>
     *
     * @param bootstrapServer the Kafka Container address
     * @return the configuration.
     */
    public static KafkaProducer<String, CloudEvent> newProducer(String bootstrapServer) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.put(ProducerConfig.RETRIES_CONFIG, 10);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        return new KafkaProducer<>(configs);}
}
