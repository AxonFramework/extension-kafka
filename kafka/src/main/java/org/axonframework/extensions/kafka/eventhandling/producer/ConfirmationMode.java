/*
 * Copyright (c) 2010-2018. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.producer;

/**
 * Modes for publishing Axon Event Messages to Kafka.
 * <ul>
 * <li>TRANSACTIONAL: use Kafka transactions while sending messages</li>
 * <li>WAIT_FOR_ACK: send messages and wait for acknowledgment</li>
 * <li>NONE: Fire and forget</li>
 * </ul>
 *
 * @author Nakul Mishra
 * @since 4.0
 */
public enum ConfirmationMode {

    /**
     * Indicates a confirmation mode which uses Kafka transactions whilst sending messages.
     */
    TRANSACTIONAL,

    /**
     * Indicates a confirmation mode which sends messages and waits for consumption acknowledgements.
     */
    WAIT_FOR_ACK,

    /**
     * Indicates a confirmation mode resembling fire and forget.
     */
    NONE;

    /**
     * Verify whether {@code this} confirmation mode is of type {@link #TRANSACTIONAL}.
     *
     * @return {@code true} if {@code this} confirmation mode matches {@link #TRANSACTIONAL}, {@code false} if it
     * doesn't
     */
    public boolean isTransactional() {
        return this == TRANSACTIONAL;
    }

    /**
     * Verify whether {@code this} confirmation mode is of type {@link #WAIT_FOR_ACK}.
     *
     * @return {@code true} if {@code this} confirmation mode matches {@link #WAIT_FOR_ACK}, {@code false} if it
     * doesn't
     */
    public boolean isWaitForAck() {
        return this == WAIT_FOR_ACK;
    }
}
