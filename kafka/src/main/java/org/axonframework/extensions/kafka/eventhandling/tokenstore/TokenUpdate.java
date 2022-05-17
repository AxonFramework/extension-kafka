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
import org.axonframework.eventhandling.tokenstore.AbstractTokenEntry;
import org.axonframework.eventhandling.tokenstore.GenericTokenEntry;

import java.time.Instant;
import java.util.UUID;

import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.*;

/**
 * The message used to persist the tokens, and to update them. It needs the Token specific information like {@link
 * #processorName}, {@link #segment}, {@link #owner} and {@link #timestamp} so we know both how to store the information
 * in memory, and are able to know if a token is claimable and such without needing to deserialize it first.
 * <p>
 * It needs the {@link #token} and {@link #tokenType} so we can deserialize the token when needed.
 * <p>
 * The {@link #sequenceNumber} is there, so we can handle concurrent writes for the same processor name and segment. In
 * those cases the on stored on the broker first, so the one with the lowest offset, wins.
 * <p>
 * To be able to know which write failed in the case of concurrent writes an {@link #id} is available. This will be used
 * by the {@link TokenStoreState} to provide feedback when updating the in memory representation of the state.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
class TokenUpdate {

    private final UUID id;
    private final String processorName;
    private final int segment;
    private final String owner;
    private final byte[] token;
    private final String tokenType;
    private final Instant timestamp;
    private final long sequenceNumber;

    private static final String ID_HEADER = "id";
    private static final String PROCESSOR_NAME_HEADER = "processorName";
    private static final String SEGMENT_HEADER = "segment";
    private static final String OWNER_HEADER = "owner";
    private static final String TOKEN_TYPE_HEADER = "tokenType";
    private static final String TIMESTAMP_HEADER = "timestamp";
    private static final String SEQUENCE_NUMBER_HEADER = "sequenceNumber";

    /**
     * Used to create an update to a previous one, where the sequence number should be higher, or with zero for a new
     * update.
     *
     * @param tokenEntry     the token entry to store
     * @param sequenceNumber the sequence number
     */
    TokenUpdate(AbstractTokenEntry<byte[]> tokenEntry, long sequenceNumber) {
        this.id = UUID.randomUUID();
        this.processorName = tokenEntry.getProcessorName();
        this.segment = tokenEntry.getSegment();
        this.owner = tokenEntry.getOwner();
        this.token = tokenEntry.getSerializedToken() == null ? new byte[0] : tokenEntry.getSerializedToken().getData();
        this.tokenType = tokenEntry.getSerializedToken() == null ?
                null : tokenEntry.getSerializedToken().getType().getName();
        this.timestamp = tokenEntry.timestamp();
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Used by the {@link TokenUpdateDeserializer} when reading the updates from Kafka.
     *
     * @param headers the Kafka headers
     * @param data    the serialized token data
     */
    TokenUpdate(Headers headers, byte[] data) {
        this.id = UUID.fromString(valueAsString(headers, ID_HEADER));
        this.processorName = valueAsString(headers, PROCESSOR_NAME_HEADER);
        this.segment = valueAsInt(headers, SEGMENT_HEADER, 0);
        this.owner = valueAsString(headers, OWNER_HEADER);
        this.token = data;
        this.tokenType = valueAsString(headers, TOKEN_TYPE_HEADER);
        this.timestamp = Instant.ofEpochMilli(valueAsLong(headers, TIMESTAMP_HEADER));
        this.sequenceNumber = valueAsLong(headers, SEQUENCE_NUMBER_HEADER, 0L);
    }

    /**
     * Used to create a copy of a current update, will set the id to a different value
     *
     * @param update the update to copy from
     * @param delete whether this is an update to mark deletion, if so some fields are set to null.
     */
    TokenUpdate(TokenUpdate update, boolean delete) {
        this.id = UUID.randomUUID();
        this.processorName = update.processorName;
        this.segment = update.segment;
        this.owner = delete ? null : update.owner;
        this.token = delete ? null : update.token;
        this.tokenType = delete ? null : update.tokenType;
        this.timestamp = AbstractTokenEntry.clock.instant();
        this.sequenceNumber = update.sequenceNumber + 1;
    }

    void setHeaders(Headers headers) {
        addHeader(headers, ID_HEADER, id);
        addHeader(headers, PROCESSOR_NAME_HEADER, processorName);
        addHeader(headers, SEGMENT_HEADER, segment);
        addHeader(headers, OWNER_HEADER, owner);
        addHeader(headers, TOKEN_TYPE_HEADER, tokenType);
        addHeader(headers, TIMESTAMP_HEADER, timestamp);
        addHeader(headers, SEQUENCE_NUMBER_HEADER, sequenceNumber);
    }

    AbstractTokenEntry<byte[]> toTokenEntry() {
        return new GenericTokenEntry<>(
                token.length == 0 ? null : token,
                tokenType,
                timestamp.toString(),
                owner,
                processorName,
                segment,
                byte[].class
        );
    }

    UUID getId() {
        return id;
    }

    String getProcessorName() {
        return processorName;
    }

    int getSegment() {
        return segment;
    }

    String getOwner() {
        return owner;
    }

    byte[] getToken() {
        return token;
    }

    Instant getTimestamp() {
        return timestamp;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }
}
