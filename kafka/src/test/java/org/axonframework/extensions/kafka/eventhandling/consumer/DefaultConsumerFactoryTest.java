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
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.DEFAULT_GROUP_ID;
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.minimal;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link DefaultConsumerFactory}, asserting construction and utilization of the class.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(topics = {"testCreatedConsumer_ValidConfig_CanCommunicateToKafka"}, partitions = 1)
public class DefaultConsumerFactoryTest {

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    private ProducerFactory<String, String> producerFactory;
    private Consumer<?, ?> testConsumer;

    @Before
    public void setUp() {
        producerFactory = producerFactory(kafkaBroker);
        testConsumer = mock(Consumer.class);
    }

    @After
    public void tearDown() {
        producerFactory.shutDown();
        testConsumer.close();
    }

    @Test(expected = AxonConfigurationException.class)
    public void testCreateConsumerInvalidConfig() {
        new DefaultConsumerFactory<>(null);
    }

    @Test
    public void testCreatedConsumerValidConfigCanCommunicateToKafka() {
        String testTopic = "testCreatedConsumer_ValidConfig_CanCommunicateToKafka";

        Producer<String, String> testProducer = producerFactory.createProducer();
        testProducer.send(new ProducerRecord<>(testTopic, 0, null, null, "foo"));
        testProducer.flush();

        ConsumerFactory<?, ?> testSubject = new DefaultConsumerFactory<>(minimal(kafkaBroker));
        testConsumer = testSubject.createConsumer(DEFAULT_GROUP_ID);
        testConsumer.subscribe(Collections.singleton(testTopic));

        assertThat(KafkaTestUtils.getRecords(testConsumer).count()).isOne();
    }
}