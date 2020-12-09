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
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.SortedKafkaMessageBuffer;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.serialization.Serializer;
import org.axonframework.spring.config.AxonConfiguration;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.axonframework.springboot.autoconfig.InfraConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.invoke.MethodHandles;
import java.util.Collections;

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
@AutoConfigureBefore(InfraConfiguration.class)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
    public ProducerFactory<String, byte[]> kafkaProducerFactory() {
        ConfirmationMode confirmationMode = properties.getPublisher().getConfirmationMode();
        String transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();

        DefaultProducerFactory.Builder<String, byte[]> builder = DefaultProducerFactory.<String, byte[]>builder()
                .configuration(properties.buildProducerProperties())
                .confirmationMode(confirmationMode);

        if (isNonEmptyString(transactionIdPrefix)) {
            builder.transactionalIdPrefix(transactionIdPrefix)
                   .confirmationMode(ConfirmationMode.TRANSACTIONAL);
            if (!confirmationMode.isTransactional()) {
                logger.warn(
                        "The confirmation mode is set to [{}], whilst a transactional id prefix is present. "
                                + "The transactional id prefix overwrites the confirmation mode choice to TRANSACTIONAL",
                        confirmationMode
                );
            }
        }

        return builder.build();
    }

    private boolean isNonEmptyString(String s) {
        return s != null && !s.equals("");
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
                                 .assignHandlerTypesMatching(
                                         DEFAULT_PROCESSING_GROUP,
                                         clazz -> clazz.isAssignableFrom(KafkaEventPublisher.class)
                                 );

        /*
         * TODO: Remove the following line after upgrading Axon Framework to release 4.4.3 or higher
         *
         * Prior to the Axon Framework 4.4.3 release, an instance selector must be assigned as type selectors are
         * ignored. After version 4.4.3, this behaviour has changed and therefore upgrading to 4.4.3 or later releases
         * will make the following assignment redundant.
         *
         * For more information see:
         *    https://github.com/AxonFramework/extension-kafka/issues/84
         *    https://github.com/AxonFramework/AxonFramework/commit/e6249f13e71e70e71c187320bd7ecd1401ac8fbc
         */
        eventProcessingConfigurer.assignHandlerInstancesMatching(DEFAULT_PROCESSING_GROUP, kafkaEventPublisher::equals);

        KafkaProperties.EventProcessorMode processorMode = kafkaProperties.getProducer().getEventProcessorMode();
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
    @ConditionalOnProperty(value = "axon.kafka.consumer.event-processor-mode", havingValue = "TRACKING")
    public StreamableKafkaMessageSource<String, byte[]> streamableKafkaMessageSource(
            ConsumerFactory<String, byte[]> kafkaConsumerFactory,
            Fetcher<String, byte[], KafkaEventMessage> kafkaFetcher,
            KafkaMessageConverter<String, byte[]> kafkaMessageConverter
    ) {
        return StreamableKafkaMessageSource.<String, byte[]>builder()
                .topics(Collections.singletonList(properties.getDefaultTopic()))
                .consumerFactory(kafkaConsumerFactory)
                .fetcher(kafkaFetcher)
                .messageConverter(kafkaMessageConverter)
                .bufferFactory(() -> new SortedKafkaMessageBuffer<>(properties.getFetcher().getBufferSize()))
                .build();
    }
}
