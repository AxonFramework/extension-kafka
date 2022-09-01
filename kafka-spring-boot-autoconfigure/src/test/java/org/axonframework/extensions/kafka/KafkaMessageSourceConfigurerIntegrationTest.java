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

package org.axonframework.extensions.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.axonframework.config.Configurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.extensions.kafka.configuration.KafkaMessageSourceConfigurer;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;
import org.axonframework.serialization.Serializer;
import org.axonframework.springboot.autoconfig.AxonServerAutoConfiguration;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.test.context.TestPropertySource;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@TestPropertySource("classpath:application-source-configurer.properties")
class KafkaMessageSourceConfigurerIntegrationTest {

    public static final String EVENT_GROUP = "SourceConfigurerIntegrationTest_Group";
    public static final String KAFKA_TOPIC = "Axon.Events";
    public static final String GROUP_ID = "test-consumer-group";
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final ConsumerFactory<String, byte[]> kafkaConsumerFactory = mock(ConsumerFactory.class);

    private static final Consumer<String, byte[]> consumer = spy(new MockConsumer<>(OffsetResetStrategy.NONE));

    @BeforeAll
    static void setup() {
        when(kafkaConsumerFactory.createConsumer(GROUP_ID)).thenReturn(consumer);
    }

    @Test
    void shouldStartConsuming() {
        await()
                .atMost(Duration.of(5L, ChronoUnit.SECONDS))
                .untilAsserted(() -> verify(consumer, atLeastOnce()).poll(any(Duration.class)));
    }

    @Component
    @ProcessingGroup(EVENT_GROUP)
    public static class EventLogger {

        @EventHandler
        public void on(Object event) {
            logger.info("Received: {}", event);
        }
    }

    @SpringBootApplication(exclude = AxonServerAutoConfiguration.class)
    static class Application {

        @Bean
        public EventStore eventStore() {
            return EmbeddedEventStore.builder().storageEngine(new InMemoryEventStorageEngine()).build();
        }

        @Bean
        public TokenStore tokenStore() {
            return new InMemoryTokenStore();
        }

        @Bean
        public KafkaMessageSourceConfigurer kafkaMessageSourceConfigurer() {
            return new KafkaMessageSourceConfigurer();
        }

        @Bean
        public SubscribableKafkaMessageSource<String, byte[]> streamableKafkaMessageSource(
                @Qualifier("eventSerializer") Serializer eventSerializer,
                Fetcher<String, byte[], EventMessage<?>> kafkaFetcher,
                KafkaMessageConverter<String, byte[]> kafkaMessageConverter
        ) {
            return SubscribableKafkaMessageSource
                    .<String, byte[]>builder()
                    .topics(Collections.singletonList(KAFKA_TOPIC))
                    .groupId(GROUP_ID)
                    .serializer(eventSerializer)
                    .consumerFactory(kafkaConsumerFactory)
                    .fetcher(kafkaFetcher)
                    .messageConverter(kafkaMessageConverter)
                    .build();
        }

        @Autowired
        public void configure(
                Configurer configurer,
                KafkaMessageSourceConfigurer kafkaMessageSourceConfigurer,
                SubscribableKafkaMessageSource<String, byte[]> source
        ) {
            kafkaMessageSourceConfigurer.configureSubscribableSource(c -> source);
            configurer.registerModule(kafkaMessageSourceConfigurer);
            configurer.eventProcessing().registerSubscribingEventProcessor(EVENT_GROUP, c -> source);
        }
    }
}
