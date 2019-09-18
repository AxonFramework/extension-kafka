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
import org.axonframework.extensions.kafka.eventhandling.consumer.KafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaSendingEventHandler;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.axonframework.extensions.kafka.eventhandling.ConsumerConfigUtil.minimal;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(topics = { "integration" }, partitions = 5, controlledShutdown = true)
public class KafkaIntegrationTest {

    @Autowired
    private KafkaEmbedded kafka;

    private Configurer configurer = DefaultConfigurer.defaultConfiguration();

    private EventBus eventBus;
    private ProducerFactory<String, byte[]> producerFactory;
    private KafkaPublisher<String, byte[]> publisher;
    private Fetcher fetcher;

    @Before
    public void setupComponents() {
        producerFactory = ProducerConfigUtil.ackProducerFactory(kafka, ByteArraySerializer.class);
        publisher = KafkaPublisher.<String, byte[]>builder()
            .producerFactory(producerFactory)
            .topic("integration")
            .build();
        KafkaSendingEventHandler sender = new KafkaSendingEventHandler(publisher);
        // event processor for listening to event bus and writing to kafka
        configurer.eventProcessing(eventProcessingConfigurer -> eventProcessingConfigurer.registerEventHandler(c -> sender));

        ConsumerFactory<String, byte[]> consumerFactory =
            new DefaultConsumerFactory<>(minimal(kafka, "consumer1", ByteArrayDeserializer.class));

        fetcher = AsyncFetcher.<String, byte[]>builder()
            .consumerFactory(consumerFactory)
            .topic("integration")
            .pollTimeout(300, TimeUnit.MILLISECONDS)
            .build();

        // event bus
        eventBus = SimpleEventBus.builder().build();
        configurer.configureEventBus(configuration -> eventBus);

        configurer.start();
    }

    @After
    public void shutdown() {
        producerFactory.shutDown();
        fetcher.shutdown();
        publisher.shutDown();
    }

    @Test
    public void testPublishAndReadMessages() throws Exception {


        KafkaMessageSource messageSource = new KafkaMessageSource(fetcher);
        BlockingStream<TrackedEventMessage<?>> stream1 = messageSource.openStream(null);
        stream1.close();
        BlockingStream<TrackedEventMessage<?>> stream2 = messageSource.openStream(null);

        eventBus.publish(asEventMessage("test"));

        // the consumer may need some time to start
        assertTrue(stream2.hasNextAvailable(25, TimeUnit.SECONDS));
        TrackedEventMessage<?> actual = stream2.nextAvailable();
        assertNotNull(actual);

        stream2.close();
    }
}
