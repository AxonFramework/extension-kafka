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
@TestPropertySource(properties = "axon.kafka.producer.event-processor-mode=SUBSCRIBING")
class SubscribingProducerIntegrationTest {

    @MockBean
    private KafkaPublisher kafkaPublisher;

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
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
            threadId = Thread.currentThread().getId();
            latch.countDown();
            return null;
        }

        public Long getThreadId() throws InterruptedException {
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
