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

package org.axonframework.extensions.kafka.autoconfig;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.KafkaEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.SortedKafkaMessageBuffer;
import org.axonframework.extensions.kafka.eventhandling.consumer.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.serialization.Serializer;
import org.axonframework.spring.config.AxonConfiguration;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher.DEFAULT_PROCESSING_GROUP;

/**
 * Auto configuration for the Axon Kafka Extension as an Event Message distribution solution.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
@Configuration
@ConditionalOnClass(KafkaPublisher.class)
@AutoConfigureAfter(AxonAutoConfiguration.class)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

    private final KafkaProperties properties;

    public KafkaAutoConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaMessageConverter<String, byte[]> kafkaMessageConverter(
            @Qualifier("eventSerializer") Serializer eventSerializer) {
        return DefaultKafkaMessageConverter.builder().serializer(eventSerializer).build();
    }

    @Bean("axonKafkaProducerFactory")
    @ConditionalOnMissingBean
    @ConditionalOnProperty("axon.kafka.producer.transaction-id-prefix")
    public ProducerFactory<String, byte[]> kafkaProducerFactory() {
        Map<String, Object> producer = properties.buildProducerProperties();
        String transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix == null) {
            throw new IllegalStateException("transactionalIdPrefix cannot be empty");
        }
        return DefaultProducerFactory.<String, byte[]>builder()
                .configuration(producer)
                .confirmationMode(ConfirmationMode.TRANSACTIONAL)
                .transactionalIdPrefix(transactionIdPrefix)
                .build();
    }

    @ConditionalOnMissingBean
    @Bean(destroyMethod = "shutDown")
    @ConditionalOnBean({ProducerFactory.class, KafkaMessageConverter.class})
    public KafkaPublisher<String, byte[]> kafkaPublisher(ProducerFactory<String, byte[]> axonKafkaProducerFactory,
                                                         KafkaMessageConverter<String, byte[]> kafkaMessageConverter,
                                                         AxonConfiguration configuration) {
        return KafkaPublisher.<String, byte[]>builder()
                .producerFactory(axonKafkaProducerFactory)
                .messageConverter(kafkaMessageConverter)
                .messageMonitor(configuration.messageMonitor(KafkaPublisher.class, "kafkaPublisher"))
                .topic(properties.getDefaultTopic())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({KafkaPublisher.class})
    public KafkaEventPublisher<String, byte[]> kafkaEventPublisher(KafkaPublisher<String, byte[]> kafkaPublisher,
                                                                   KafkaProperties kafkaProperties,
                                                                   EventProcessingConfigurer eventProcessingConfigurer) {
        KafkaEventPublisher<String, byte[]> kafkaEventPublisher =
                KafkaEventPublisher.<String, byte[]>builder().kafkaPublisher(kafkaPublisher).build();

        /*
         * Register an invocation error handler which re-throws any exception.
         * This will ensure a TrackingEventProcessor to enter the error mode which will retry, and it will ensure the
         * SubscribingEventProcessor to bubble the exception to the callee. For more information see
         *  https://docs.axoniq.io/reference-guide/configuring-infrastructure-components/event-processing/event-processors#error-handling
         */
        eventProcessingConfigurer.registerEventHandler(configuration -> kafkaEventPublisher)
                                 .registerListenerInvocationErrorHandler(
                                         DEFAULT_PROCESSING_GROUP, configuration -> PropagatingErrorHandler.instance()
                                 )
                                 .assignHandlerInstancesMatching(
                                         DEFAULT_PROCESSING_GROUP,
                                         eventHandler -> eventHandler.getClass().equals(KafkaEventPublisher.class)
                                 );

        KafkaProperties.EventProcessorMode processorMode = kafkaProperties.getEventProcessorMode();
        if (processorMode == KafkaProperties.EventProcessorMode.SUBSCRIBING) {
            eventProcessingConfigurer.registerSubscribingEventProcessor(DEFAULT_PROCESSING_GROUP);
        } else if (processorMode == KafkaProperties.EventProcessorMode.TRACKING) {
            eventProcessingConfigurer.registerTrackingEventProcessor(DEFAULT_PROCESSING_GROUP);
        } else {
            throw new AxonConfigurationException("Unknown Event Processor Mode [" + processorMode + "] detected");
        }

        return kafkaEventPublisher;
    }

    @Bean("axonKafkaConsumerFactory")
    @ConditionalOnMissingBean
    public ConsumerFactory<String, byte[]> kafkaConsumerFactory() {
        return new DefaultConsumerFactory<>(properties.buildConsumerProperties());
    }

    @ConditionalOnMissingBean
    @Bean(destroyMethod = "shutdown")
    public Fetcher<?, ?, ?> kafkaFetcher() {
        return AsyncFetcher.builder()
                           .pollTimeout(properties.getFetcher().getPollTimeout())
                           .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({ConsumerFactory.class, KafkaMessageConverter.class, Fetcher.class})
    public StreamableKafkaMessageSource<String, byte[]> streamableKafkaMessageSource(
            ConsumerFactory<String, byte[]> kafkaConsumerFactory,
            Fetcher<KafkaEventMessage, String, byte[]> kafkaFetcher,
            KafkaMessageConverter<String, byte[]> kafkaMessageConverter
    ) {
        return StreamableKafkaMessageSource.<String, byte[]>builder()
                .topic(properties.getDefaultTopic())
                .consumerFactory(kafkaConsumerFactory)
                .fetcher(kafkaFetcher)
                .messageConverter(kafkaMessageConverter)
                .bufferFactory(() -> new SortedKafkaMessageBuffer<>(properties.getFetcher().getBufferSize()))
                .build();
    }
}
