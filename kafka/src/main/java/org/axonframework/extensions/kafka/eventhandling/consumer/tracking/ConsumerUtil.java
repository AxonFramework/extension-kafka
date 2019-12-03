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

package org.axonframework.extensions.kafka.eventhandling.consumer.tracking;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Utility class to simplify working with a {@link Consumer}. For example used to subscribe a Consumer to a topic.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public abstract class ConsumerUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerUtil.class);

    private ConsumerUtil() {
        // Utility class
    }

    /**
     * Subscribes the {@link Consumer} to a particular {@link org.apache.kafka.common.internals.Topic}, using a private
     * {@link ConsumerRebalanceListener} implementation to update the (by the Consumer) used offsets of each partition
     * present in the given {@link KafkaTrackingToken} whenever a re-balance happens.
     *
     * @param topic    the topic to subscribe the given {@code consumer} to
     * @param consumer the {@link Consumer} to subscribe to the given {@code topic} and for which the offsets should be
     *                 adjusted upon a rebalance operation
     * @param token    the token containing all {@code partition}/{@code offsets} pairs which should be updated for the
     *                 configured {@code consumer} upon a Consumer rebalance operation
     */
    public static void seek(String topic, Consumer consumer, KafkaTrackingToken token) {
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Not implemented
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (KafkaTrackingToken.isEmpty(token)) {
                    return;
                }

                logger.debug("Seeking offsets for Consumer corresponding to the partitions in token: [{}]", token);

                token.partitionPositions().forEach(
                        (partition, offset) -> consumer.seek(KafkaTrackingToken.partition(topic, partition), offset + 1)
                );
            }
        });
    }
}
