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
package org.axonframework.extensions.kafka.eventhandling;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.KafkaEventMessage;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.minimal;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka Integration tests asserting a message can be published through a Producer on a Kafka topic and received through
 * a Consumer.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class KafkaIntegrationTest extends KafkaContainerTest {

    private static final String TEST_TOPIC = "integration";
    private static final Integer NR_PARTITIONS = 5;
    private final Configurer configurer = DefaultConfigurer.defaultConfiguration();
    private EventBus eventBus;
    private ProducerFactory<String, byte[]> producerFactory;
    private KafkaPublisher<String, byte[]> publisher;
    private Fetcher<String, byte[], KafkaEventMessage> fetcher;
    private ConsumerFactory<String, byte[]> consumerFactory;

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TEST_TOPIC);
        KafkaAdminUtils.createPartitions(getBootstrapServers(), NR_PARTITIONS, TEST_TOPIC);
    }

    @AfterAll
    public static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TEST_TOPIC);
    }

    @BeforeEach
    void setUp() {
        producerFactory = ProducerConfigUtil.ackProducerFactory(getBootstrapServers(), ByteArraySerializer.class);
        publisher = KafkaPublisher.<String, byte[]>builder()
                                  .producerFactory(producerFactory)
                                  .topicResolver(m -> {
                                      if (m.getPayloadType().isAssignableFrom(String.class)) {
                                          return Optional.of(TEST_TOPIC);
                                      } else {
                                          return Optional.empty();
                                      }
                                  })
                                  .build();
        KafkaEventPublisher<String, byte[]> sender =
                KafkaEventPublisher.<String, byte[]>builder().kafkaPublisher(publisher).build();
        configurer.eventProcessing(
                eventProcessingConfigurer -> eventProcessingConfigurer.registerEventHandler(c -> sender)
        );

        consumerFactory = new DefaultConsumerFactory<>(minimal(getBootstrapServers(), ByteArrayDeserializer.class));

        fetcher = AsyncFetcher.<String, byte[], KafkaEventMessage>builder()
                              .pollTimeout(300)
                              .build();

        eventBus = SimpleEventBus.builder().build();
        configurer.configureEventBus(configuration -> eventBus);

        configurer.start();
    }

    @AfterEach
    void shutdown() {
        producerFactory.shutDown();
        fetcher.shutdown();
        publisher.shutDown();
    }

    @Test
    void testPublishAndReadMessages() throws Exception {
        StreamableKafkaMessageSource<String, byte[]> streamableMessageSource =
                StreamableKafkaMessageSource.<String, byte[]>builder()
                                            .topics(Collections.singletonList(TEST_TOPIC))
                                            .consumerFactory(consumerFactory)
                                            .fetcher(fetcher)
                                            .build();

        BlockingStream<TrackedEventMessage<?>> stream1 = streamableMessageSource.openStream(null);
        stream1.close();
        BlockingStream<TrackedEventMessage<?>> stream2 = streamableMessageSource.openStream(null);

        eventBus.publish(asEventMessage("test"));

        // The consumer may need some time to start
        assertTrue(stream2.hasNextAvailable(25, TimeUnit.SECONDS));
        TrackedEventMessage<?> actual = stream2.nextAvailable();
        assertNotNull(actual);
        assertEquals("test", actual.getPayload());

        stream2.close();
    }

    @Test
    void testSkipPublishForLongPayload() throws Exception {
        StreamableKafkaMessageSource<String, byte[]> streamableMessageSource =
                StreamableKafkaMessageSource.<String, byte[]>builder()
                                            .topics(Collections.singletonList(TEST_TOPIC))
                                            .consumerFactory(consumerFactory)
                                            .fetcher(fetcher)
                                            .build();

        BlockingStream<TrackedEventMessage<?>> stream1 = streamableMessageSource.openStream(null);
        stream1.close();
        BlockingStream<TrackedEventMessage<?>> stream2 = streamableMessageSource.openStream(null);

        //This one will not be received
        eventBus.publish(asEventMessage(42L));
        //Added so we don't have to wait longer than necessary, to know the other one did not publish
        eventBus.publish(asEventMessage("test"));

        // The consumer may need some time to start
        assertTrue(stream2.hasNextAvailable(25, TimeUnit.SECONDS));
        TrackedEventMessage<?> actual = stream2.nextAvailable();
        assertNotNull(actual);
        assertInstanceOf(String.class, actual.getPayload(), "Long is not skipped");

        stream2.close();
    }
}
