/*
 * Copyright (c) 2010-2018. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaAdminUtils;
import org.axonframework.extensions.kafka.eventhandling.util.KafkaContainerTest;
import org.junit.jupiter.api.*;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;

import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.minimal;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link DefaultConsumerFactory}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class DefaultConsumerFactoryTest extends KafkaContainerTest {

    private static final String TEST_TOPIC = "testCreatedConsumer_ValidConfig_CanCommunicateToKafka";

    private ProducerFactory<String, String> producerFactory;
    private Consumer<?, ?> testConsumer;

    @BeforeAll
    static void before() {
        KafkaAdminUtils.createTopics(getBootstrapServers(), TEST_TOPIC);
    }

    @AfterAll
    public static void after() {
        KafkaAdminUtils.deleteTopics(getBootstrapServers(), TEST_TOPIC);
    }

    @BeforeEach
    void setUp() {
        producerFactory = producerFactory(getBootstrapServers());
        testConsumer = mock(Consumer.class);
    }

    @AfterEach
    void tearDown() {
        producerFactory.shutDown();
        testConsumer.close();
    }

    @Test
    void testCreateConsumerInvalidConfig() {
        assertThrows(AxonConfigurationException.class, () -> new DefaultConsumerFactory<>(null));
    }

    @Test
    void testCreatedConsumerValidConfigCanCommunicateToKafka() {
        String testTopic = "testCreatedConsumer_ValidConfig_CanCommunicateToKafka";

        Producer<String, String> testProducer = producerFactory.createProducer();
        testProducer.send(new ProducerRecord<>(testTopic, 0, null, null, "foo"));
        testProducer.flush();

        ConsumerFactory<?, ?> testSubject = new DefaultConsumerFactory<>(minimal(KAFKA_CONTAINER
                                                                                         .getBootstrapServers()));
        testConsumer = testSubject.createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(Collections.singleton(testTopic));

        assertEquals(1, KafkaTestUtils.getRecords(testConsumer).count());
    }
}
