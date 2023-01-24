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

import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.axonframework.eventsourcing.utils.EventStoreTestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class StreamableKafkaSourceIntegrationTest {

    @Container
    private static final RedpandaContainer REDPANDA_CONTAINER = new RedpandaContainer(
            "docker.redpanda.com/vectorized/redpanda:v22.2.1");
    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner();
    }

    @Test
    void messageSendViaKafkaShouldBeReceived() {
        testApplicationContext
                .withPropertyValues("axon.axonserver.enabled=false")
                .withPropertyValues("axon.kafka.fetcher.enabled=true")
                .withPropertyValues("axon.kafka.consumer.event-processor-mode=tracking")
                .withPropertyValues("axon.kafka.producer.bootstrap-servers=" + REDPANDA_CONTAINER.getBootstrapServers())
                .withPropertyValues("axon.kafka.consumer.bootstrap-servers=" + REDPANDA_CONTAINER.getBootstrapServers())
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    EventGateway eventGateway = context.getBean(EventGateway.class);
                    assertNotNull(eventGateway);
                    publishEvent(eventGateway);
                    StreamableKafkaMessageSource<String, byte[]> messageSource = context.getBean(
                            StreamableKafkaMessageSource.class);
                    assertNotNull(messageSource);
                    receiveMessage(messageSource);
                });
    }

    private void publishEvent(EventGateway eventGateway) {
        DomainEventMessage<String> event = createEvent();
        eventGateway.publish(event);
    }

    private void receiveMessage(StreamableKafkaMessageSource<String, byte[]> messageSource)
            throws InterruptedException {
        BlockingStream<TrackedEventMessage<?>> stream = messageSource.openStream(null);
        await().atMost(Duration.ofSeconds(5L)).until(stream::hasNextAvailable);
        TrackedEventMessage<?> message = stream.nextAvailable();
        assertNotNull(message);
        assertEquals("payload", message.getPayload());
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}
