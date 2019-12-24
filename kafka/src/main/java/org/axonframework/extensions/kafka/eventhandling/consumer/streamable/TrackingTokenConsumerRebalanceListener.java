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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A {@link ConsumerRebalanceListener} which upon {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
 * will perform a {@link Consumer#seek(TopicPartition, long)} using the partition offsets in the given {@link
 * KafkaTrackingToken}.
 * <p>
 * This implementation ensures that Axon is in charge of event handling progress by always starting from the beginning
 * of the stream for unknown partitions. This approach follows how a {@link org.axonframework.eventhandling.TrackingEventProcessor}
 * deals with unknown progress in any {@link org.axonframework.eventhandling.TrackingToken}.
 *
 * @param <K> the key of the records the {@link Consumer} polls
 * @param <V> the value type of the records the {@link Consumer} polls
 * @author Steven van Beelen
 * @since 4.0
 */
public class TrackingTokenConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Consumer<K, V> consumer;
    private final Supplier<KafkaTrackingToken> tokenSupplier;

    /**
     * Create a {@link ConsumerRebalanceListener} which uses a {@link KafkaTrackingToken} through the given {@code
     * tokenSupplier} to consciously {@link Consumer#seek(TopicPartition, long)} the offsets of the partitions assigned
     * to the given {@code consumer}.
     *
     * @param consumer      the {@link Consumer} used to call {@link Consumer#seek(TopicPartition, long)} on
     * @param tokenSupplier the {@link Supplier} of a {@link KafkaTrackingToken}. This should provide the most recent
     *                      progress of the given {@code consumer}
     */
    public TrackingTokenConsumerRebalanceListener(Consumer<K, V> consumer, Supplier<KafkaTrackingToken> tokenSupplier) {
        this.consumer = consumer;
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Not implemented
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation will use {@link Consumer#seek(TopicPartition, long)} for all given {@code assignedPartitions}
     * using the offsets known in the {@link KafkaTrackingToken}, retrieved through the {@code tokenSupplier}.
     * <p>
     * If no offset is known for a given {@link TopicPartition} then we enforce the offset to {@code 0} to ensure all
     * known records for the new partition are processed. This could occur if polling is started for the first time or
     * if the number of partitions for the {@link Consumer}s topic is administratively adjusted.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        KafkaTrackingToken currentToken = tokenSupplier.get();
        assignedPartitions.forEach(assignedPartition -> {
            Map<TopicPartition, Long> tokenPartitionPositions = currentToken.positions();

            long offset = 0L;
            if (tokenPartitionPositions.containsKey(assignedPartition)) {
                offset = tokenPartitionPositions.get(assignedPartition) + 1;
            }

            logger.info("Seeking topic-partition [{}] with offset [{}]", assignedPartition, offset);
            consumer.seek(assignedPartition, offset);
        });
    }
}
