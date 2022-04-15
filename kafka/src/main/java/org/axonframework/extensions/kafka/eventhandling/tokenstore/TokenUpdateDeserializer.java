/*
 * Copyright (c) 2010-2022. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.tokenstore;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Kafka deserializer used for the {@link KafkaTokenStore}.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class TokenUpdateDeserializer implements Deserializer<TokenUpdate> {

    /**
     * Deserializes the bytes to a {@link TokenUpdate} object
     *
     * @param topic the topic the bytes are read from, part of the interface, currently only used for logging.
     * @param bytes the bytes received from the Kafka broker.
     * @return a {@link NotSupportedException} exception
     */
    @Override
    public TokenUpdate deserialize(String topic, byte[] bytes) {
        throw new NotSupportedException("deserialize should be called also using the headers");
    }

    /**
     * Deserializes the bytes to a {@link TokenUpdate} object
     *
     * @param topic   the topic the bytes are read from, part of the interface, currently only used for logging.
     * @param headers the headers received from the Kafka broker.
     * @param data    the bytes received from the Kafka broker.
     * @return a {@link TokenUpdate} object
     */
    @Override
    public TokenUpdate deserialize(String topic, Headers headers, byte[] data) {
        return new TokenUpdate(headers, data);
    }
}
