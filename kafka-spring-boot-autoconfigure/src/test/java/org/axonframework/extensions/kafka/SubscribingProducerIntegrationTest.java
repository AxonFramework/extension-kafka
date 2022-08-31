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

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.springboot.autoconfig.AxonServerAutoConfiguration;
import org.junit.jupiter.api.*;
import org.mockito.invocation.*;
import org.mockito.stubbing.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@TestPropertySource("classpath:application-subscribing.properties")
class SubscribingProducerIntegrationTest {

    @MockBean
    private KafkaPublisher<?, ?> kafkaPublisher;

    @Autowired
    private EventBus eventBus;

    @Test
    void shouldPublishMessagesSynchronously() throws Exception {
        // given
        ThreadIdCaptor threadIdCaptor = new ThreadIdCaptor();
        doAnswer(threadIdCaptor).when(kafkaPublisher).send(any());

        // when
        eventBus.publish(new GenericEventMessage<>("test"));

        // then
        assertEquals(threadIdCaptor.getThreadId(), Thread.currentThread().getId());
    }

    private static class ThreadIdCaptor implements Answer<Void> {

        private static final int TIMEOUT = 10;

        private Long threadId;
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public Void answer(InvocationOnMock invocationOnMock) {
            threadId = Thread.currentThread().getId();
            latch.countDown();
            return null;
        }

        public Long getThreadId() throws InterruptedException {
            //noinspection ResultOfMethodCallIgnored
            latch.await(TIMEOUT, TimeUnit.SECONDS);
            if (threadId == null) {
                throw new IllegalStateException("Unable to capture thread id in " + TIMEOUT + " minutes.");
            }
            return threadId;
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
    }
}
