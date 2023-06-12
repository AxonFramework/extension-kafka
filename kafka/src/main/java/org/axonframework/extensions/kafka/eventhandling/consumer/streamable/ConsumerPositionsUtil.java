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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.extensions.kafka.eventhandling.consumer.KafkaSubscriber;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

import static org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerSeekUtil.topicPartitions;

/**
 * Contains static util functions related to the Kafka consumer to find the correct positions.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
class ConsumerPositionsUtil {

    private ConsumerPositionsUtil() {
        //prevent instantiation
    }

    static Map<TopicPartition, Long> getPositionsBasedOnTime(
            @Nonnull Consumer<?, ?> consumer,
            @Nonnull KafkaSubscriber subscriber,
            @Nonnull Instant rawDefaultAt
    ) {
        List<TopicPartition> all = topicPartitions(consumer, subscriber);
        Map<TopicPartition, Long> positions = new HashMap<>();
        OffsetSupplier offsetSupplier = new OffsetSupplier(consumer, rawDefaultAt, all);
        all.forEach(assignedPartition -> {
            Long offset = offsetSupplier.getOffset(assignedPartition);
            //if it's 0, we otherwise miss the first event
            if (offset > 1) {
                positions.put(assignedPartition, offset - 1);
            }
        });
        return positions;
    }

    static Map<TopicPartition, Long> getHeadPositions(
            @Nonnull Consumer<?, ?> consumer,
            @Nonnull KafkaSubscriber subscriber
    ) {
        List<TopicPartition> all = topicPartitions(consumer, subscriber);
        Map<TopicPartition, Long> positions = new HashMap<>();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(all);
        endOffsets.forEach((assignedPartition, offset) -> {
            //if it's 0, we otherwise miss the first event
            if (offset > 1) {
                positions.put(assignedPartition, offset - 1);
            }
        });
        return positions;
    }

    private static class OffsetSupplier {

        private final Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap;
        private final Map<TopicPartition, Long> endOffsets;

        private OffsetSupplier(Consumer<?, ?> consumer, Instant rawDefaultAt, List<TopicPartition> all) {
            long defaultAt = rawDefaultAt.toEpochMilli();
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            all.forEach(tp -> timestampsToSearch.put(tp, defaultAt));
            partitionOffsetMap = consumer.offsetsForTimes(timestampsToSearch);
            endOffsets = consumer.endOffsets(all);
        }

        private Optional<Long> getDefaultOffset(TopicPartition assignedPartition) {
            return Optional.ofNullable(partitionOffsetMap.get(assignedPartition))
                           .map(OffsetAndTimestamp::offset);
        }

        private long getEndOffset(TopicPartition assignedPartition) {
            return endOffsets.get(assignedPartition);
        }

        private Long getOffset(TopicPartition assignedPartition) {
            return getDefaultOffset(assignedPartition).orElseGet(() -> getEndOffset(assignedPartition));
        }
    }
}
