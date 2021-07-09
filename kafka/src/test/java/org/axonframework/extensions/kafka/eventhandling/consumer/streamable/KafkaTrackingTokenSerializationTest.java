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
import org.axonframework.eventhandling.ReplayToken;
import org.axonframework.eventhandling.TrackingToken;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Collections.singletonMap;
import static org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken.newInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(Parameterized.class)
public class KafkaTrackingTokenSerializationTest {


    private static final String TEST_TOPIC = "topic";
    private static final int TEST_PARTITION = 0;
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    private final TestSerializer serializer;

    public KafkaTrackingTokenSerializationTest(TestSerializer serializer) {
        this.serializer = serializer;
    }
    
    @Parameterized.Parameters(name = "{index} {0}")
    public static Collection<TestSerializer> serializers() {
       return TestSerializer.all();
    }

    @Test
    public void testReplayTokenShouldBeSerializable() {
        ReplayToken tokenReset = new ReplayToken(nonEmptyToken(TEST_TOPIC_PARTITION, 0L));
        KafkaTrackingToken tokenStart = nonEmptyToken(TEST_TOPIC_PARTITION, 1L);
        ReplayToken replayToken = new ReplayToken(tokenReset, tokenStart);
        String serializeReplayToken = serializer.serialize(replayToken);
        TrackingToken deserializeReplayToken = serializer.deserialize(serializeReplayToken, ReplayToken.class);
        assertTrue(deserializeReplayToken.equals(replayToken));
    }

    @Test
    public void testTokenShouldBeSerializable(){
        KafkaTrackingToken token = nonEmptyToken(TEST_TOPIC_PARTITION, 0L);
        String serializeCopy = serializer.serialize(token);
        KafkaTrackingToken deserializeCopy = serializer.deserialize(serializeCopy, KafkaTrackingToken.class);
        assertTrue(deserializeCopy.equals(token));
    }


    public static KafkaTrackingToken nonEmptyToken(TopicPartition topic, Long pos) {
        return newInstance(singletonMap(topic, pos));
    }

}