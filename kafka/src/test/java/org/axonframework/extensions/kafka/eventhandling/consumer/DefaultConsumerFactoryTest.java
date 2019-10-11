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
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
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
import static org.axonframework.extensions.kafka.eventhandling.util.ConsumerConfigUtil.minimal;
import static org.axonframework.extensions.kafka.eventhandling.util.ProducerConfigUtil.producerFactory;

/**
 * Tests for the {@link DefaultProducerFactory}.
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

    @Test(expected = AxonConfigurationException.class)
    public void testCreateConsumerInvalidConfig() {
        new DefaultConsumerFactory<>(null);
    }

    @Test
    public void testCreatedConsumerValidConfigCanCommunicateToKafka() {
        String topic = "testCreatedConsumer_ValidConfig_CanCommunicateToKafka";
        ProducerFactory<String, String> pf = producerFactory(kafkaBroker);
        Producer<String, String> producer = pf.createProducer();
        producer.send(new ProducerRecord<>(topic, 0, null, null, "foo"));
        producer.flush();
        DefaultConsumerFactory<Object, Object> testSubject = new DefaultConsumerFactory<>(minimal(kafkaBroker, topic));
        Consumer<Object, Object> consumer = testSubject.createConsumer();
        consumer.subscribe(Collections.singleton(topic));

        assertThat(KafkaTestUtils.getRecords(consumer).count()).isOne();

        consumer.close();
        pf.shutDown();
    }
}