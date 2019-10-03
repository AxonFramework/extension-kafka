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

@SuppressWarnings("UNUSED")
@ProcessingGroup(KafkaSendingEventHandler.GROUP)
public class KafkaSendingEventHandler {

    public static final String GROUP = "axon.kafka.event";

    private final KafkaPublisher kafkaPublisher;

    public KafkaSendingEventHandler(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @EventHandler
    public <T> void handle(EventMessage<T> message) {
        kafkaPublisher.send(message);
    }
}
