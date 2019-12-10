/*
 * Copyright (c) 2010-2019. Axon Framework
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
package org.axonframework.extension.kafka.example

import org.axonframework.config.Configurer
import org.axonframework.config.EventProcessingConfigurer
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.extensions.kafka.KafkaProperties
import org.axonframework.extensions.kafka.configuration.KafkaMessageSourceConfigurer
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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
}

/**
 * If the Consumer Processor Mode is set to Tracking, that's when we register a
 * [org.axonframework.eventhandling.TrackingEventProcessor] using the [StreamableKafkaMessageSource] which the auto
 * configuration creates.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "TRACKING")
class TrackingConfiguration {

    @Autowired
    fun configureStreamableKafkaSource(configurer: EventProcessingConfigurer,
                                       streamableKafkaMessageSource: StreamableKafkaMessageSource<String, ByteArray>) {
        configurer.registerTrackingEventProcessor("kafka-group") { streamableKafkaMessageSource }
    }
}

/**
 * If the Consumer Processor Mode is set to Subscribing, that's when we register a
 * [org.axonframework.eventhandling.SubscribingEventProcessor] using the [SubscribableKafkaMessageSource] which this
 * Configuration class builds.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "SUBSCRIBING")
class SubscribingConfiguration {

    /**
     * To start a [SubscribableKafkaMessageSource] at the right point in time, we should add those sources to a
     * [KafkaMessageSourceConfigurer].
     */
    @Bean
    fun kafkaMessageSourceConfigurer() = KafkaMessageSourceConfigurer()

    /**
     * The [KafkaMessageSourceConfigurer] should be added to Axon's [Configurer] to ensure it will be called upon start up.
     */
    @Autowired
    fun registerKafkaMessageSourceConfigurer(configurer: Configurer,
                                             kafkaMessageSourceConfigurer: KafkaMessageSourceConfigurer) {
        configurer.registerModule(kafkaMessageSourceConfigurer)
    }

    /**
     * The auto configuration currently does not create a [SubscribableKafkaMessageSource] bean because the user is
     * inclined to provide the group-id in all scenarios. Doing so provides users the option to create several
     * [org.axonframework.eventhandling.SubscribingEventProcessor] beans belonging to the same group, thus giving
     * Kafka the opportunity to balance the load.
     *
     * Additionally, this subscribable source should be added to the [KafkaMessageSourceConfigurer] to ensure it will
     * be started and stopped within the configuration lifecycle.
     */
    @Bean
    fun subscribableKafkaMessageSource(
            kafkaProperties: KafkaProperties,
            consumerFactory: ConsumerFactory<String, ByteArray>,
            fetcher: Fetcher<String, ByteArray, EventMessage<*>>,
            messageConverter: KafkaMessageConverter<String, ByteArray>,
            kafkaMessageSourceConfigurer: KafkaMessageSourceConfigurer
    ): SubscribableKafkaMessageSource<String, ByteArray> {
        val subscribableKafkaMessageSource = SubscribableKafkaMessageSource.builder<String, ByteArray>()
                .topics(listOf(kafkaProperties.defaultTopic))
                .groupId("kafka-group")
                .consumerFactory(consumerFactory)
                .fetcher(fetcher)
                .messageConverter(messageConverter)
                .build()
        kafkaMessageSourceConfigurer.registerSubscribableSource { subscribableKafkaMessageSource }
        return subscribableKafkaMessageSource
    }

    @Autowired
    fun configureSubscribableKafkaSource(eventProcessingConfigurer: EventProcessingConfigurer,
                                         subscribableKafkaMessageSource: SubscribableKafkaMessageSource<String, ByteArray>) {
        eventProcessingConfigurer.registerSubscribingEventProcessor("kafka-group") { subscribableKafkaMessageSource }
    }
}