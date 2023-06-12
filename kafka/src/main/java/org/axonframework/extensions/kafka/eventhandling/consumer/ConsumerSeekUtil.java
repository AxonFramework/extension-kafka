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
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Contains static util functions related to the Kafka consumer related to seeking to certain offsets.
 *
 * @author Gerard Klijs
 * @since 4.5.4
 */
public class ConsumerSeekUtil {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ConsumerSeekUtil() {
        //prevent instantiation
    }

    /**
     * Assigns the correct {@link TopicPartition partitions} to the consumer, and seeks to the correct offset, using the
     * {@link KafkaTrackingToken}, defaulting to the head of the partition. So for each {@link TopicPartition partition}
     * that belongs to the {@code topics}, either it will start reading from the next record of the partition, if
     * included in the token, or else from the start.
     *
     * @param consumer      a Kafka consumer instance
     * @param tokenSupplier a function that returns the current {@link KafkaTrackingToken}
     * @param subscriber    a {@link KafkaSubscriber} that contains the topics to subscribe to.
     */
    public static void seekToCurrentPositions(Consumer<?, ?> consumer, Supplier<KafkaTrackingToken> tokenSupplier,
                                              KafkaSubscriber subscriber) {
        List<TopicPartition> all = topicPartitions(consumer, subscriber);
        consumer.assign(all);
        KafkaTrackingToken currentToken = tokenSupplier.get();
        Map<TopicPartition, Long> tokenPartitionPositions = currentToken.getPositions();
        all.forEach(assignedPartition -> {
            long offset = 0L;
            if (tokenPartitionPositions.containsKey(assignedPartition)) {
                offset = tokenPartitionPositions.get(assignedPartition) + 1;
            }

            logger.info("Seeking topic-partition [{}] with offset [{}]", assignedPartition, offset);
            consumer.seek(assignedPartition, offset);
        });
    }

    /**
     * Get all the {@link TopicPartition topicPartitions} belonging to the given {@code topics}.
     *
     * @param consumer      a Kafka {@link Consumer}
     * @param subscriber    a {@link KafkaSubscriber} that contains the topics to subscribe to.
     * @return a list of all the {@link TopicPartition topicPartitions}
     */
    public static List<TopicPartition> topicPartitions(Consumer<?, ?> consumer, KafkaSubscriber subscriber) {
        return consumer.listTopics().entrySet()
                       .stream()
                       .filter(e -> subscriber.subscribesToTopicName(e.getKey()))
                       .flatMap(e -> e.getValue().stream())
                       .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                                                                partitionInfo.partition()))
                       .collect(Collectors.toList());
    }
}
