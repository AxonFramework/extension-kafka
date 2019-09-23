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
package org.axonframework.extension.kafka.example

import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.extensions.kafka.KafkaProperties
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling


fun main(args: Array<String>) {
    SpringApplication.run(KafkaAxonExampleApplication::class.java, *args)
}

@SpringBootApplication
@EnableScheduling
class KafkaAxonExampleApplication {

    @Bean
    fun eventStore() = EmbeddedEventStore.builder().storageEngine(InMemoryEventStorageEngine()).build()

    @Bean
    fun tokenStore() = InMemoryTokenStore()

    @Bean
    fun producerFactory(kafkaProperties: KafkaProperties): ProducerFactory<String, ByteArray>? {
        return DefaultProducerFactory.builder<String, ByteArray>()
                .configuration(kafkaProperties.buildProducerProperties())
                .producerCacheSize(10_000)
                .confirmationMode(ConfirmationMode.WAIT_FOR_ACK)
                .build()
    }

}