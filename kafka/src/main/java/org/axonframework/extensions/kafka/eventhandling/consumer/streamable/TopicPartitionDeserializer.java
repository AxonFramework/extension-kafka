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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.serialization.SerializationException;

/**
 * Custom {@link KeyDeserializer} used to deserialize the {@link TopicPartition}.
 *
 * @author leechedan
 * @since 4.0
 */
public class TopicPartitionDeserializer extends KeyDeserializer {

    @Override
    public TopicPartition deserializeKey(String key, DeserializationContext context) {

        if (null == key || key.lastIndexOf('-') < 1) {
            return null;
        }
        int i = key.lastIndexOf('-');
        String posStr = key.substring(i + 1);
        int pos = 0;
        try {
            pos = Integer.valueOf(posStr);
        } catch (NumberFormatException e) {
            throw new SerializationException(String.format("cannot parse the pos of TopicPartition from json:[%s]",
                                                           key));
        }
        if (pos < 0) {
            throw new SerializationException("pos of TopicPartition should be greater than zero");
        }
        return new TopicPartition(key.substring(0, i), pos);
    }
}
