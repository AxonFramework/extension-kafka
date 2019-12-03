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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.eventhandling.TrackingToken;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.axonframework.common.Assert.isTrue;

/**
 * A {@link TrackingToken} implementation dedicated to tracking all consumed Kafka records which have been committed by
 * another Axon Kafka Publisher.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaTrackingToken implements TrackingToken, Serializable {

    private final Map<Integer, Long> partitionPositions;

    /**
     * Returns a new {@link KafkaTrackingToken} instance based on the given {@code partitionPositions}.
     *
     * @param partitionPositions a {@link Map} from {@link Integer} to {@link Long}, specifying the offset of every
     *                           partition
     * @return a new tracking token based on the given {@code partitionPositions}
     */
    @JsonCreator
    public static KafkaTrackingToken newInstance(
            @JsonProperty("partitionPositions") Map<Integer, Long> partitionPositions) {
        return new KafkaTrackingToken(partitionPositions);
    }

    /**
     * Create an empty {@link KafkaTrackingToken} instance.
     *
     * @return an empty {@link KafkaTrackingToken} instance
     */
    public static KafkaTrackingToken emptyToken() {
        return newInstance(new HashMap<>());
    }

    private KafkaTrackingToken(Map<Integer, Long> partitionPositions) {
        this.partitionPositions = Collections.unmodifiableMap(new HashMap<>(partitionPositions));
    }

    /**
     * Retrieve the {@link Map} stored in this {@link TrackingToken} containing partition-offset pairs corresponding the
     * actual offsets per partition a Consumer is fetching records from.
     *
     * @return the {@link Map} stored in this {@link TrackingToken} containing partition-offset pairs corresponding the
     * actual offsets per partition a Consumer is fetching records from
     */
    @SuppressWarnings("WeakerAccess")
    public Map<Integer, Long> partitionPositions() {
        return partitionPositions;
    }

    /**
     * Creates a {@link Collection} of {@link TopicPartition}s for each {@code partition} present in {@code this} token.
     * Uses the given {@code topic} to finalize the TopicPartitions
     *
     * @param topic the topic for which {@link TopicPartition} instances should be created
     * @return a {@link Collection} of {@link TopicPartition}s for each {@code partition} present in {@code this} token
     */
    @SuppressWarnings("WeakerAccess")
    public Collection<TopicPartition> partitions(String topic) {
        return partitionPositions.keySet()
                                 .stream()
                                 .map(partition -> partition(topic, partition))
                                 .collect(Collectors.toList());
    }

    /**
     * Create a {@link TopicPartition} based on the given {@code topic} and {@code partitionNumber}.
     *
     * @param topic           the topic for which a {@link TopicPartition} should be created
     * @param partitionNumber the partition number for which a {@link TopicPartition} should be created
     * @return a {@link TopicPartition} based on the given {@code topic} and {@code partitionNumber}
     */
    public static TopicPartition partition(String topic, int partitionNumber) {
        return new TopicPartition(topic, partitionNumber);
    }

    /**
     * Advance {@code this} token's partition-offset pairs. If the given {@code partition} already exists, the current
     * entry's {@code offset} is replaced for the given {@code offset}. Returns a new {@link KafkaTrackingToken}
     * instance.
     *
     * @param partition the partition number for which the {@code offset} should be advanced
     * @param offset    the offset corresponding to the given {@code partition} to advance
     * @return a new and advanced {@link KafkaTrackingToken} instance
     */
    @SuppressWarnings("WeakerAccess")
    public KafkaTrackingToken advancedTo(int partition, long offset) {
        isTrue(partition >= 0, () -> "Partition may not be negative");
        isTrue(offset >= 0, () -> "Offset may not be negative");

        Map<Integer, Long> updatedPartitionPositions = new HashMap<>(partitionPositions);
        updatedPartitionPositions.put(partition, offset);
        return new KafkaTrackingToken(updatedPartitionPositions);
    }

    @SuppressWarnings("ConstantConditions") // Verified TrackingToken type through `Assert.isTrue` operation
    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");

        return new KafkaTrackingToken(bounds((KafkaTrackingToken) other, Math::min));
    }

    @SuppressWarnings("ConstantConditions") // Verified TrackingToken type through `Assert.isTrue` operation
    @Override
    public TrackingToken upperBound(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");

        return new KafkaTrackingToken(bounds((KafkaTrackingToken) other, Math::max));
    }

    private Map<Integer, Long> bounds(KafkaTrackingToken token, BiFunction<Long, Long, Long> boundsFunction) {
        Map<Integer, Long> intersection = new HashMap<>(this.partitionPositions);
        token.partitionPositions().forEach(intersection::putIfAbsent);

        intersection.keySet()
                    .forEach(partitionNumber -> intersection.put(partitionNumber, boundsFunction.apply(
                            this.partitionPositions().getOrDefault(partitionNumber, 0L),
                            token.partitionPositions().getOrDefault(partitionNumber, 0L))

                    ));
        return intersection;
    }

    @SuppressWarnings("ConstantConditions") // Verified TrackingToken type through `Assert.isTrue` operation
    @Override
    public boolean covers(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");
        KafkaTrackingToken otherToken = (KafkaTrackingToken) other;

        return otherToken.partitionPositions
                .entrySet().stream()
                .allMatch(position -> position.getValue() <= this.partitionPositions
                        .getOrDefault(position.getKey(), -1L));
    }

    /**
     * Verify whether the given {@code token} is not empty, thus whether it contains partition-offset pairs.
     *
     * @param token the {@link KafkaTrackingToken} to verify for not being empty
     * @return {@code true} if the given {@code token} contains partition-offset pairs and {@code false} if it doesn't
     */
    @SuppressWarnings("WeakerAccess")
    public static boolean isNotEmpty(KafkaTrackingToken token) {
        return !isEmpty(token);
    }

    /**
     * Verify whether the given {@code token} is {@code null} or contains zero partition-offset pairs.
     *
     * @param token the {@link KafkaTrackingToken} to verify for being empty
     * @return {@code true} if the given {@code token} is {@code null} or empty and {@code false} if it isn't
     */
    public static boolean isEmpty(KafkaTrackingToken token) {
        return token == null || token.partitionPositions.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaTrackingToken that = (KafkaTrackingToken) o;
        return Objects.equals(partitionPositions, that.partitionPositions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionPositions);
    }

    @Override
    public String toString() {
        return "KafkaTrackingToken{" +
                "partitionPositions=" + partitionPositions +
                '}';
    }
}
