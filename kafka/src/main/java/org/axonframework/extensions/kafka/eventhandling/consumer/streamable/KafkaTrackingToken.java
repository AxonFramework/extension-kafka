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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.eventhandling.TrackingToken;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

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

    private final Map<TopicPartition, Long> positions;

    private KafkaTrackingToken(Map<TopicPartition, Long> positions) {
        this.positions = Collections.unmodifiableMap(new HashMap<>(positions));
    }

    /**
     * Returns a new {@link KafkaTrackingToken} instance based on the given {@code partitionPositions}.
     *
     * @param positions a {@link Map} from {@link Integer} to {@link Long}, specifying the offset of every
     *                                partition
     * @return a new tracking token based on the given {@code partitionPositions}
     */
    @JsonCreator
    public static KafkaTrackingToken newInstance(@JsonProperty("positions") Map<TopicPartition, Long> positions) {
        return new KafkaTrackingToken(positions);
    }

    /**
     * Create an empty {@link KafkaTrackingToken} instance.
     *
     * @return an empty {@link KafkaTrackingToken} instance
     */
    public static KafkaTrackingToken emptyToken() {
        return newInstance(new HashMap<>());
    }

    /**
     * Verify whether the given {@code token} is not empty, thus whether it contains partition-offset pairs.
     *
     * @param token the {@link KafkaTrackingToken} to verify for not being empty
     * @return {@code true} if the given {@code token} contains partition-offset pairs and {@code false} if it doesn't
     */
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
        return token == null || token.positions.isEmpty();
    }

    /**
     * Attempts to convert the given {@code trackingToken} to a KafkaTrackingToken. If {@code null}, an empty
     * KafkaTrackingToken is returned, otherwise a cast is attempted.
     * <p>
     * The returned value is never {@code null}.
     *
     * @param trackingToken The token to convert
     * @return a KafkaTrackingToken instances representing the same position as the given {@code trackingToken}
     * @throws IllegalArgumentException if the given {@code trackingToken} is non-null and of an unsupported type
     */
    @SuppressWarnings("ConstantConditions")
    public static KafkaTrackingToken from(TrackingToken trackingToken) {
        isTrue(trackingToken == null || trackingToken instanceof KafkaTrackingToken,
               () -> "Incompatible token type provided.");
        return trackingToken == null ? KafkaTrackingToken.emptyToken() : (KafkaTrackingToken) trackingToken;
    }

    /**
     * Retrieve the {@link TopicPartition}/offset {@link Map} stored in this {@link TrackingToken}. This collection
     * corresponds to the actual progress of a Consumer polling for records.
     *
     * @return the {@link TopicPartition}/offset {@link Map} stored in this {@link TrackingToken}
     */
    public Map<TopicPartition, Long> positions() {
        return positions;
    }

    /**
     * Advance {@code this} token's offset-per-{@link TopicPartition} pairs. If the given {@code topic}/{@code
     * partition} already exists, the current entry's {@code offset} is replaced for the given {@code offset}. Returns a
     * new {@link KafkaTrackingToken} instance.
     *
     * @param topic     a {@link String} defining the topic for which the {@code offset} advanced
     * @param partition the partition number tied to the given {@code topic} for which the {@code offset} advanced
     * @param offset    the offset corresponding to the given {@code topic} and {@code partition} which advanced
     * @return a new advanced {@link KafkaTrackingToken} instance
     */
    public KafkaTrackingToken advancedTo(String topic, int partition, long offset) {
        isTrue(topic != null && !topic.equals(""), () -> "Topic should be a non-empty string");
        isTrue(partition >= 0, () -> "Partition may not be negative");
        isTrue(offset >= 0, () -> "Offset may not be negative");

        return advancedTo(new TopicPartition(topic, partition), offset);
    }

    private KafkaTrackingToken advancedTo(TopicPartition topicPartition, long offset) {
        Map<TopicPartition, Long> updatedPositions = new HashMap<>(positions());
        updatedPositions.put(topicPartition, offset);
        return new KafkaTrackingToken(updatedPositions);
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");
        //noinspection ConstantConditions - Verified cast through `Assert.isTrue` operation
        return new KafkaTrackingToken(bounds((KafkaTrackingToken) other, Math::min));
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");
        //noinspection ConstantConditions - Verified cast through `Assert.isTrue` operation
        return new KafkaTrackingToken(bounds((KafkaTrackingToken) other, Math::max));
    }

    private Map<TopicPartition, Long> bounds(KafkaTrackingToken other, BiFunction<Long, Long, Long> boundsFunction) {
        Map<TopicPartition, Long> intersection = new HashMap<>(positions());
        other.positions().forEach(intersection::putIfAbsent);

        intersection.keySet()
                    .forEach(topicPartition -> intersection.put(topicPartition, boundsFunction.apply(
                            this.positions().getOrDefault(topicPartition, 0L),
                            other.positions().getOrDefault(topicPartition, 0L))
                    ));
        return intersection;
    }

    @Override
    public boolean covers(TrackingToken other) {
        isTrue(other instanceof KafkaTrackingToken, () -> "Incompatible token type provided.");
        //noinspection ConstantConditions - Verified cast through `Assert.isTrue` operation
        KafkaTrackingToken otherToken = (KafkaTrackingToken) other;

        return otherToken.positions()
                         .entrySet().stream()
                         .allMatch(offsetsEntry -> match(offsetsEntry.getKey(), offsetsEntry.getValue()));
    }

    private boolean match(TopicPartition otherTopicPartition, long otherOffset) {
        return otherOffset <= this.positions().getOrDefault(otherTopicPartition, -1L);
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
        return Objects.equals(positions, that.positions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(positions);
    }

    @Override
    public String toString() {
        return "KafkaTrackingToken{" +
                "positions=" + positions +
                '}';
    }
}
