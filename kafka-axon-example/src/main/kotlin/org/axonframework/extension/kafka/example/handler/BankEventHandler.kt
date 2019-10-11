/*
 * Copyright (c) 2019. Axon Framework
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

package org.axonframework.extension.kafka.example.handler

import mu.KLogging
import org.axonframework.config.ProcessingGroup
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.EventMessage
import org.springframework.stereotype.Component

/**
 * Collecting event handler for logging, connected to a Kafka consumer with the processing group name "kafka-group".
 * Further configured in the [org.axonframework.extension.kafka.example.KafkaAxonExampleApplication].
 */
@Component
@ProcessingGroup("kafka-group")
class BankEventHandler {

    companion object : KLogging()

    /**
     * Receive all events and log them.
     */
    @EventHandler
    fun <T : EventMessage<Any>> on(event: T) {
        logger.info { "received event ${event.payload}" }
    }

}