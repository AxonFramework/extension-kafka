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

import org.apache.kafka.common.TopicPartition;
import org.axonframework.eventhandling.ReplayToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaTrackingToken.newInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract tests serialization the {@link KafkaTrackingToken}.
 *
 * @author leechedan
 * @author leechedan
 */
public abstract class AbstractKafkaTrackingTokenSerializeTest {

    private static final String TEST_TOPIC = "topic";
    private static final int TEST_PARTITION = 0;
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    protected Serializer serializer;


    @Test
    void testReplaySerialize(){
        ReplayToken tokenReset = new ReplayToken(nonEmptyToken(TEST_TOPIC_PARTITION, 0L));
        KafkaTrackingToken tokenStart = nonEmptyToken(TEST_TOPIC_PARTITION, 1L);
        TrackingToken replayToken = new ReplayToken(tokenReset, tokenStart);
        SerializedObject<byte[]> serializeReplayToken = serializer.serialize(replayToken, byte[].class);
        TrackingToken deserializeReplayToken = serializer.deserialize(serializeReplayToken);
        assertTrue(deserializeReplayToken.equals(replayToken));
    }

    @Test
    void testTokenSerialize(){
        KafkaTrackingToken token = nonEmptyToken(TEST_TOPIC_PARTITION, 0L);
        SerializedObject<byte[]> serializeCopy = serializer.serialize(token, byte[].class);
        KafkaTrackingToken deserializeCopy = serializer.deserialize(serializeCopy);
        assertTrue(deserializeCopy.equals(token));
    }


    private static KafkaTrackingToken nonEmptyToken(TopicPartition topic, Long pos) {
        return newInstance(singletonMap(topic, pos));
    }

}
