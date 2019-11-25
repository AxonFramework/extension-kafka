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

import org.axonframework.config.EventProcessingConfigurer
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.extensions.kafka.KafkaProperties
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter
import org.axonframework.extensions.kafka.eventhandling.consumer.*
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * Starting point.
 */
fun main(args: Array<String>) {
    SpringApplication.run(KafkaAxonExampleApplication::class.java, *args)
}

/**
 * Main application class.
 */
@SpringBootApplication
@EnableScheduling
class KafkaAxonExampleApplication {

    /**
     * Configures to use in-memory embedded event store.
     */
    @Bean
    fun eventStore() = EmbeddedEventStore.builder().storageEngine(InMemoryEventStorageEngine()).build()

    /**
     * Configures to us in-memory token store.
     */
    @Bean
    fun tokenStore() = InMemoryTokenStore()

    /**
     * Creates a Kafka producer factory, using the Kafka properties configured in resources/application.yml
     */
    @Bean
    fun producerFactory(kafkaProperties: KafkaProperties): ProducerFactory<String, ByteArray>? {
        return DefaultProducerFactory.builder<String, ByteArray>()
                .configuration(kafkaProperties.buildProducerProperties())
                .producerCacheSize(10_000)
                .confirmationMode(ConfirmationMode.WAIT_FOR_ACK)
                .build()
    }

    @Autowired
    fun configureKafkaSourceForProcessingGroup(configurer: EventProcessingConfigurer,
                                               kafkaProperties: KafkaProperties,
                                               consumerFactory: ConsumerFactory<String, ByteArray>,
                                               fetcher: Fetcher<KafkaEventMessage, String, ByteArray>,
                                               kafkaMessageConverter: KafkaMessageConverter<String, ByteArray>) {
        val streamableKafkaMessageSource = StreamableKafkaMessageSource.builder<String, ByteArray>()
                .topic(kafkaProperties.defaultTopic)
                .groupId("kafka-group")
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .messageConverter(kafkaMessageConverter)
                .bufferFactory { SortedKafkaMessageBuffer<KafkaEventMessage>(kafkaProperties.fetcher.bufferSize) }
                .build()
        configurer.registerTrackingEventProcessor("kafka-group") { streamableKafkaMessageSource }
    }
}