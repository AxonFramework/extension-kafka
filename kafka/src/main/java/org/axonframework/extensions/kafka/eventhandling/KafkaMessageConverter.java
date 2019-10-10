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

package org.axonframework.extensions.kafka.eventhandling;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.eventhandling.EventMessage;

import java.util.Optional;

/**
 * Converts Kafka records from Axon {@link EventMessage}s and vice versa.
 *
 * @param <K> the key type of the Kafka record
 * @param <V> the value type of the Kafka record
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public interface KafkaMessageConverter<K, V> {

    /**
     * Creates a {@link ProducerRecord} for a given {@link EventMessage} to be published on a Kafka Producer.
     *
     * @param eventMessage the event message to convert into a {@link ProducerRecord} for Kafka
     * @param topic        the Kafka topic to publish the message on
     * @return the converted {@code eventMessage} as a {@link ProducerRecord}
     */
    ProducerRecord<K, V> createKafkaMessage(EventMessage<?> eventMessage, String topic);

    /**
     * Reconstruct an {@link EventMessage} from the given  {@link ConsumerRecord}. The returned optional
     * resolves to a message if the given input parameters represented a correct EventMessage.
     *
     * @param consumerRecord the Event Message represented inside Kafka
     * @return the converted {@code consumerRecord} as an {@link EventMessage}
     */
    Optional<EventMessage<?>> readKafkaMessage(ConsumerRecord<K, V> consumerRecord);
}
