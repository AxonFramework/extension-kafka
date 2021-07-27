/*
 * Copyright (c) 2010-2021. Axon Framework
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

import org.apache.kafka.common.TopicPartition;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@link TopicPartitionDeserializer}.
 *
 * @author leechedan
 */
public class TopicPartitionDeserializerTest {

    private static final List<TopicPartition> TOPIC_PARTITIONS = Arrays.asList(
            new TopicPartition("local", 0),
            new TopicPartition("local-", 1),
            new TopicPartition("local-event", 100)
    );

    private final TopicPartitionDeserializer testSubject = new TopicPartitionDeserializer();

    @Test
    public void testDeserializeShouldSuccess() {
        TOPIC_PARTITIONS.forEach(
                item -> assertEquals(item, testSubject.deserializeKey(item.toString(), null), item + " fail")
        );
    }
}
