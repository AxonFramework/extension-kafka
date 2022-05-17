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
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka serializer used for the {@link TokenUpdate}.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class TokenUpdateSerializer implements Serializer<TokenUpdate> {

    /**
     * This method should not be used, instead {@link #serialize(String, Headers, TokenUpdate)}) serialize}, with
     * headers should be used.
     *
     * @param topic       topic the bytes are written to, part of the interface, currently not used
     * @param tokenUpdate the token update object to send to Kafka
     * @return a {@link UnsupportedOperationException} exception
     */
    @Override
    @SuppressWarnings("squid:S1168") //needs to return null to work as tombstone
    public byte[] serialize(String topic, TokenUpdate tokenUpdate) {
        throw new UnsupportedOperationException("serialize should be called also using the headers");
    }

    /**
     * Serializes the {@code tokenUpdate} to bytes.
     *
     * @param topic       topic the bytes are written to, part of the interface, currently not used
     * @param headers     kafka headers
     * @param tokenUpdate the token update object to send to Kafka
     * @return the bytes to add to the Kafka record
     */
    @Override
    public byte[] serialize(String topic, Headers headers, TokenUpdate tokenUpdate) {
        tokenUpdate.setHeaders(headers);
        return tokenUpdate.getToken();
    }
}
