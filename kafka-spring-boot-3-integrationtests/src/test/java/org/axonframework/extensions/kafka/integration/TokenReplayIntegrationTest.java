/*
 * Copyright (c) 2010-2023. Axon Framework
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

package org.axonframework.extensions.kafka.integration;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.config.Configurer;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.ResetHandler;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.newProducer;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TokenReplayIntegrationTest {

    @Container
    private static final RedpandaContainer REDPANDA_CONTAINER = new RedpandaContainer(
            "docker.redpanda.com/vectorized/redpanda:v22.2.1");
    private ApplicationContextRunner testApplicationContext;


    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues("axon.kafka.fetcher.enabled=true")
                .withPropertyValues("axon.kafka.publisher.enabled=false")
                .withPropertyValues("axon.kafka.message-converter-mode=cloud_event")
                .withPropertyValues("axon.kafka.consumer.event-processor-mode=tracking")
                .withPropertyValues("axon.kafka.consumer.bootstrap-servers=" + REDPANDA_CONTAINER.getBootstrapServers())
                .withUserConfiguration(DefaultContext.class);
    }

    @Test
    void afterResetShouldOnlyProcessTenEventsIfTimeSetMidway() {
        testApplicationContext
                .withPropertyValues("axon.kafka.default-topic=counterfeed-1")
                .run(context -> {
                    Counter counter = context.getBean(Counter.class);
                    assertNotNull(counter);
                    assertEquals(0, counter.getCount());
                    Instant between = addRecords("counterfeed-1");
                    await().atMost(Duration.ofSeconds(5L)).untilAsserted(
                            () -> assertEquals(20, counter.getCount())
                    );
                    EventProcessingConfiguration processingConfiguration = context.getBean(EventProcessingConfiguration.class);
                    assertNotNull(processingConfiguration);
                    processingConfiguration
                            .eventProcessorByProcessingGroup(
                                    "counterfeedprocessor",
                                    TrackingEventProcessor.class
                            )
                            .ifPresent(tep -> {
                                tep.shutDown();
                                tep.resetTokens(tep.getMessageSource().createTokenAt(between));
                                assertEquals(0, counter.getCount());
                                tep.start();
                            });
                    await().atMost(Duration.ofSeconds(5L)).untilAsserted(
                            () -> assertEquals(10, counter.getCount())
                    );
                });
    }

    @Test
    void afterResetShouldOnlyProcessNewMessages() {
        testApplicationContext
                .withPropertyValues("axon.kafka.default-topic=counterfeed-2")
                .run(context -> {
                    Counter counter = context.getBean(Counter.class);
                    assertNotNull(counter);
                    assertEquals(0, counter.getCount());
                    addRecords("counterfeed-2");
                    await().atMost(Duration.ofSeconds(5L)).untilAsserted(
                            () -> assertEquals(20, counter.getCount())
                    );
                    EventProcessingConfiguration processingConfiguration = context.getBean(EventProcessingConfiguration.class);
                    assertNotNull(processingConfiguration);
                    processingConfiguration
                            .eventProcessorByProcessingGroup(
                                    "counterfeedprocessor",
                                    TrackingEventProcessor.class
                            )
                            .ifPresent(tep -> {
                                tep.shutDown();
                                tep.resetTokens(tep.getMessageSource().createHeadToken());
                                assertEquals(0, counter.getCount());
                                tep.start();
                            });
                    addRecords("counterfeed-2");
                    await().atMost(Duration.ofSeconds(5L)).untilAsserted(
                            () -> assertEquals(20, counter.getCount())
                    );
                });
    }

    private Instant addRecords(String topic) {
        Producer<String, CloudEvent> producer = newProducer(REDPANDA_CONTAINER.getBootstrapServers());
        sendTenMessages(producer, topic);
        Instant now = Instant.now();
        sendTenMessages(producer, topic);
        producer.close();
        return now;
    }

    private void sendMessage(Producer<String, CloudEvent> producer, String topic) {
        CloudEvent event = new CloudEventBuilder()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("source"))
                .withData("Payload".getBytes())
                .withType("java.util.String")
                .build();
        ProducerRecord<String, CloudEvent> record = new ProducerRecord<>(topic, 0, null, null, event);
        producer.send(record);
    }

    private void sendTenMessages(Producer<String, CloudEvent> producer, String topic) {
        IntStream.range(0, 10).forEach(i -> sendMessage(producer, topic));
        producer.flush();
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

        @Bean
        Counter counter() {
            return new Counter();
        }

        @Bean
        KafkaEventHandler kafkaEventHandler(Counter counter) {
            return new KafkaEventHandler(counter);
        }

        @Autowired
        public void registerProcessor(
                Configurer configurer,
                StreamableKafkaMessageSource<?, ?> streamableKafkaMessageSource
        ) {
            configurer.eventProcessing()
                      .registerTrackingEventProcessor("counterfeedprocessor", c -> streamableKafkaMessageSource);
        }
    }

    private static class Counter {

        private final AtomicInteger counter = new AtomicInteger();

        int getCount() {
            return counter.get();
        }

        void count() {
            counter.incrementAndGet();
        }

        void reset() {
            counter.set(0);
        }
    }

    @SuppressWarnings("unused")
    @Component
    @ProcessingGroup("counterfeedprocessor")
    private static class KafkaEventHandler {

        private final Counter counter;

        private KafkaEventHandler(Counter counter) {
            this.counter = counter;
        }

        @EventHandler
        void on(EventMessage<?> eventMessage) {
            counter.count();
        }

        @ResetHandler
        void onReset() {
            counter.reset();
        }
    }
}
