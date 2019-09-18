/*
 * Copyright (c) 2010-2018. Axon Framework
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

/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.autoconfig;

import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.KafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.consumer.SortedKafkaMessageBuffer;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaSendingEventHandler;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.serialization.Serializer;
import org.axonframework.spring.config.AxonConfiguration;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.axonframework.extensions.kafka.KafkaProperties.EventProcessorMode;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Kafka.
 *
 * @author Nakul Mishra
 * @since 3.0
 */
@Configuration
@ConditionalOnClass(KafkaPublisher.class)
@EnableConfigurationProperties(KafkaProperties.class)
@AutoConfigureAfter({ AxonAutoConfiguration.class })
public class KafkaAutoConfiguration {

    private final KafkaProperties properties;

    public KafkaAutoConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @ConditionalOnMissingBean
    @ConditionalOnProperty("axon.kafka.producer.transaction-id-prefix")
    @Bean
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
    @Bean
    @ConditionalOnProperty("axon.kafka.consumer.group-id")
    public ConsumerFactory<String, byte[]> kafkaConsumerFactory() {
        return new DefaultConsumerFactory<>(properties.buildConsumerProperties());
    }

    @ConditionalOnMissingBean
    @Bean
    public KafkaMessageConverter<String, byte[]> kafkaMessageConverter(
        @Qualifier("eventSerializer") Serializer eventSerializer) {
        return DefaultKafkaMessageConverter.builder().serializer(eventSerializer).build();
    }

    @ConditionalOnMissingBean
    @Bean(destroyMethod = "shutDown")
    @ConditionalOnBean({ ProducerFactory.class, KafkaMessageConverter.class })
    public KafkaPublisher<String, byte[]> kafkaPublisher(
        ProducerFactory<String, byte[]> kafkaProducerFactory,
        KafkaMessageConverter<String, byte[]> kafkaMessageConverter,
        AxonConfiguration configuration) {
        return KafkaPublisher.<String, byte[]>builder()
            .producerFactory(kafkaProducerFactory)
            .messageConverter(kafkaMessageConverter)
            .messageMonitor(configuration.messageMonitor(KafkaPublisher.class, "kafkaPublisher"))
            .topic(properties.getDefaultTopic())
            .build();
    }

    @ConditionalOnMissingBean
    @ConditionalOnBean({ ConsumerFactory.class, KafkaMessageConverter.class })
    @Bean(destroyMethod = "shutdown")
    public Fetcher kafkaFetcher(
        ConsumerFactory<String, byte[]> kafkaConsumerFactory,
        KafkaMessageConverter<String, byte[]> kafkaMessageConverter) {
        return AsyncFetcher.<String, byte[]>builder()
            .consumerFactory(kafkaConsumerFactory)
            .bufferFactory(() -> new SortedKafkaMessageBuffer<>(properties.getFetcher().getBufferSize()))
            .messageConverter(kafkaMessageConverter)
            .topic(properties.getDefaultTopic())
            .pollTimeout(properties.getFetcher().getPollTimeout(), MILLISECONDS)
            .build();
    }

    @ConditionalOnMissingBean
    @Bean
    @ConditionalOnBean(ConsumerFactory.class)
    public KafkaMessageSource kafkaMessageSource(Fetcher kafkaFetcher) {
        return new KafkaMessageSource(kafkaFetcher);
    }

    @ConditionalOnBean({ EventProcessingConfigurer.class })
    @Autowired
    public void configureKafkaEventProcessor(
        final EventProcessingConfigurer eventProcessingConfigurer,
        final KafkaProperties kafkaProperties) {

        final EventProcessorMode mode = kafkaProperties.getEventProcessorMode();
        switch (mode) {
            case SUBSCRIBING:
                eventProcessingConfigurer.registerSubscribingEventProcessor(KafkaSendingEventHandler.GROUP);
                break;
            case TRACKING:
                eventProcessingConfigurer.registerTrackingEventProcessor(KafkaSendingEventHandler.GROUP);
                break;
            default:
                break;
        }
        /*
         * Register an invocation error handler, re-throwing exception.
         * This will lead the tracking processor to go to error mode and retry
         * and will cause the subscribing event handler to bubble the exception to the caller.
         * For more information see https://docs.axoniq.io/reference-guide/configuring-infrastructure-components/event-processing/event-processors#error-handling
         */
        eventProcessingConfigurer.registerListenerInvocationErrorHandler(
            KafkaSendingEventHandler.GROUP,
            configuration ->
                (exception, event, eventHandler) -> {
                    throw exception;
                }
        );
    }

    @ConditionalOnMissingBean
    @Bean
    @ConditionalOnBean({ KafkaPublisher.class })
    public KafkaSendingEventHandler kafkaEventHandler(KafkaPublisher<String, byte[]> kafkaPublisher) {
        return new KafkaSendingEventHandler(kafkaPublisher);
    }

}
