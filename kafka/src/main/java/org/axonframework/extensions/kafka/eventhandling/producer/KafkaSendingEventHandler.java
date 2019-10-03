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

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;

/**
 * Event handler responsible for sending messages to Kafka using provided {@link KafkaPublisher}.
 * <p>
 * This class is intentionally not a Spring Component, but is initialized during auto-configuration
 * taking the EventProcessor Mode into account.
 * </p>
 * This class is not intended to be neither sub-classed nor instantiated by the end user of the extension.
 */
@SuppressWarnings("UNUSED")
@ProcessingGroup(KafkaSendingEventHandler.GROUP)
public class KafkaSendingEventHandler {

    /**
     * Kafka Event Handler processing group.
     */
    public static final String GROUP = "axon.kafka.event";

    private final KafkaPublisher kafkaPublisher;

    /**
     * Constructs the event handler.
     * @param kafkaPublisher publisher to be used to send message to Kafka and acknowledge them.
     */
    public KafkaSendingEventHandler(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    /**
     * Main event handling method matching all events and delegating them to Kafka.
     * @param message event received.
     * @param <T> event type.
     */
    @EventHandler
    public <T> void handle(EventMessage<T> message) {
        kafkaPublisher.send(message);
    }
}
