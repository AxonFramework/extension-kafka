/*
 * Copyright (c) 2010-2021. Axon Framework
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
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
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
@ConditionalOnExpression(
        "${axon.kafka.publisher.enabled:true} or ${axon.kafka.fetcher.enabled:true}"
)
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

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    @ConditionalOnMissingBean
    public KafkaMessageConverter<String, byte[]> kafkaMessageConverter(
            @Qualifier("eventSerializer") Serializer eventSerializer
    ) {
        return DefaultKafkaMessageConverter.builder().serializer(eventSerializer).build();
    }

    @Bean("axonKafkaProducerFactory")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public ProducerFactory<String, byte[]> kafkaProducerFactory() {
        ConfirmationMode confirmationMode = properties.getPublisher().getConfirmationMode();
        String transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();

        DefaultProducerFactory.Builder<String, byte[]> builder =
                DefaultProducerFactory.<String, byte[]>builder()
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

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @ConditionalOnMissingBean
    @Bean(destroyMethod = "shutDown")
    @ConditionalOnBean({ProducerFactory.class, KafkaMessageConverter.class})
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
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

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({KafkaPublisher.class})
    @ConditionalOnProperty(name = "axon.kafka.publisher.enabled", havingValue = "true", matchIfMissing = true)
    public KafkaEventPublisher<String, byte[]> kafkaEventPublisher(KafkaPublisher<String, byte[]> kafkaPublisher,
                                                                   KafkaProperties kafkaProperties,
                                                                   EventProcessingConfigurer eventProcessingConfigurer) {
        KafkaEventPublisher<String, byte[]> kafkaEventPublisher =
                KafkaEventPublisher.<String, byte[]>builder().kafkaPublisher(kafkaPublisher).build();

        /*
         * Register an invocation error handler which re-throws any exception.
         * This will ensure a StreamingEventProcessor to enter the error mode which will retry, and it will ensure the
         * SubscribingEventProcessor to bubble the exception to the caller. For more information see
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

        KafkaProperties.EventProcessorMode processorMode = kafkaProperties.getProducer().getEventProcessorMode();
        if (processorMode == KafkaProperties.EventProcessorMode.SUBSCRIBING) {
            eventProcessingConfigurer.registerSubscribingEventProcessor(DEFAULT_PROCESSING_GROUP);
        } else if (processorMode == KafkaProperties.EventProcessorMode.TRACKING) {
            eventProcessingConfigurer.registerTrackingEventProcessor(DEFAULT_PROCESSING_GROUP);
        } else if (processorMode == KafkaProperties.EventProcessorMode.POOLED_STREAMING) {
            eventProcessingConfigurer.registerPooledStreamingEventProcessor(DEFAULT_PROCESSING_GROUP);
        } else {
            throw new AxonConfigurationException("Unknown Event Processor Mode [" + processorMode + "] detected");
        }

        return kafkaEventPublisher;
    }

    @Bean("axonKafkaConsumerFactory")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.kafka.fetcher.enabled", havingValue = "true", matchIfMissing = true)
    public ConsumerFactory<String, byte[]> kafkaConsumerFactory() {
        return new DefaultConsumerFactory<>(properties.buildConsumerProperties());
    }

    @ConditionalOnMissingBean
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(name = "axon.kafka.fetcher.enabled", havingValue = "true", matchIfMissing = true)
    public Fetcher<?, ?, ?> kafkaFetcher() {
        return AsyncFetcher.builder()
                           .pollTimeout(properties.getFetcher().getPollTimeout())
                           .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({ConsumerFactory.class, KafkaMessageConverter.class, Fetcher.class})
    @Conditional(StreamingProcessorModeCondition.class)
    @ConditionalOnProperty(name = "axon.kafka.fetcher.enabled", havingValue = "true", matchIfMissing = true)
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
                                           .bufferFactory(() -> new SortedKafkaMessageBuffer<>(
                                                   properties.getFetcher().getBufferSize()
                                           ))
                                           .build();
    }

    private static class StreamingProcessorModeCondition extends AnyNestedCondition {

        public StreamingProcessorModeCondition() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @SuppressWarnings("unused")
        @ConditionalOnProperty(name = "axon.kafka.consumer.event-processor-mode", havingValue = "tracking")
        static class TrackingCondition {

        }

        @SuppressWarnings("unused")
        @ConditionalOnProperty(name = "axon.kafka.consumer.event-processor-mode", havingValue = "pooled_streaming")
        static class PooledStreamingCondition {

        }
    }
}
