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
package org.axonframework.extensions.kafka.example

import org.axonframework.config.Configurer
import org.axonframework.config.EventProcessingConfigurer
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.extensions.kafka.KafkaProperties
import org.axonframework.extensions.kafka.configuration.KafkaMessageSourceConfigurer
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource
import org.axonframework.serialization.Serializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
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
 * Application constants
 */
const val KAFKA_GROUP = "kafka-group"

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
    fun eventStore(): EventStore = EmbeddedEventStore.builder().storageEngine(InMemoryEventStorageEngine()).build()

    /**
     * Configures to us in-memory token store.
     */
    @Bean
    fun tokenStore() = InMemoryTokenStore()
}

/**
 * If the Consumer Processor Mode is set to Tracking, that's when we register a
 * [org.axonframework.eventhandling.TrackingEventProcessor] using the [StreamableKafkaMessageSource] which the
 * autoconfiguration creates.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "tracking")
class TrackingConfiguration {

    @Autowired
    fun configureStreamableKafkaSource(
        configurer: EventProcessingConfigurer,
        streamableKafkaMessageSource: StreamableKafkaMessageSource<String, ByteArray>
    ) {
        configurer.registerTrackingEventProcessor(KAFKA_GROUP) { streamableKafkaMessageSource }
    }
}

/**
 * If the Consumer Processor Mode is set to Subscribing, that's when we register a
 * [org.axonframework.eventhandling.SubscribingEventProcessor] using the [SubscribableKafkaMessageSource] which this
 * Configuration class builds.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "subscribing")
class SubscribingBeans {

    /**
     * The autoconfiguration currently does not create a [SubscribableKafkaMessageSource] bean because the user is
     * inclined to provide the group-id in all scenarios. Doing so provides users the option to create several
     * [org.axonframework.eventhandling.SubscribingEventProcessor] beans belonging to the same group, thus giving
     * Kafka the opportunity to balance the load.
     *
     * Additionally, this subscribable source should be added to the [KafkaMessageSourceConfigurer] to ensure it will
     * be started and stopped within the configuration lifecycle.
     */
    @Bean
    fun subscribableKafkaMessageSource(
        @Qualifier("eventSerializer") eventSerializer: Serializer,
        kafkaProperties: KafkaProperties,
        consumerFactory: ConsumerFactory<String, ByteArray>,
        fetcher: Fetcher<String, ByteArray, EventMessage<*>>,
        messageConverter: KafkaMessageConverter<String, ByteArray>,
    ): SubscribableKafkaMessageSource<String, ByteArray> {
        return SubscribableKafkaMessageSource.builder<String, ByteArray>()
            .serializer(eventSerializer)
            .topics(listOf(kafkaProperties.defaultTopic))
            .groupId(KAFKA_GROUP)
            .consumerFactory(consumerFactory)
            .fetcher(fetcher)
            .messageConverter(messageConverter)
            .build()
    }

    @Bean
    fun kafkaMessageSourceConfigurer(): KafkaMessageSourceConfigurer? {
        return KafkaMessageSourceConfigurer()
    }
}

/**
 * If the Consumer Processor Mode is set to Subscribing, that's when we register a
 * [org.axonframework.eventhandling.SubscribingEventProcessor] using the [SubscribableKafkaMessageSource]. We also need to use the [KafkaMessageSourceConfigurer] correctly to ensure the source starts and stops properly.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "subscribing")
class SubscribingConfiguration {

    @Autowired
    fun configureSubscribableKafkaSource(
        configurer: Configurer,
        kafkaMessageSourceConfigurer: KafkaMessageSourceConfigurer,
        subscribableKafkaMessageSource: SubscribableKafkaMessageSource<String, ByteArray>
    ) {
        kafkaMessageSourceConfigurer.configureSubscribableSource { subscribableKafkaMessageSource }
        configurer.registerModule(kafkaMessageSourceConfigurer)
        configurer.eventProcessing().registerSubscribingEventProcessor(KAFKA_GROUP) { subscribableKafkaMessageSource }
    }
}

/**
 * If the Consumer Processor Mode is set to Pooled Streaming, that's when we register a
 * [org.axonframework.eventhandling.pooled.PooledStreamingEventProcessor] using the [StreamableKafkaMessageSource] which
 * the autoconfiguration creates.
 */
@Configuration
@ConditionalOnProperty(value = ["axon.kafka.consumer.event-processor-mode"], havingValue = "pooled_streaming")
class PooledStreamingConfiguration {

    @Autowired
    fun configureStreamableKafkaSource(
        configurer: EventProcessingConfigurer,
        streamableKafkaMessageSource: StreamableKafkaMessageSource<String, ByteArray>
    ) {
        configurer.registerPooledStreamingEventProcessor(KAFKA_GROUP) { streamableKafkaMessageSource }
    }
}