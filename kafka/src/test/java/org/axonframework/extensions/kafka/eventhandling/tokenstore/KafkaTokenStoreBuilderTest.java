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

package org.axonframework.extensions.kafka.eventhandling.tokenstore;

import org.apache.kafka.clients.CommonClientConfigs;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link KafkaTokenStore.Builder}
 *
 * @author Gerard Klijs
 */
@ExtendWith(MockitoExtension.class)
class KafkaTokenStoreBuilderTest {

    private static final Map<String, Object> configuration = new HashMap<>();
    private static final Serializer serializer = JacksonSerializer.defaultSerializer();

    @BeforeAll
    static void before() {
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    @Test
    void testBuildWithoutSerializer() {
        KafkaTokenStore.Builder builder = KafkaTokenStore
                .builder()
                .consumerConfiguration(configuration)
                .producerConfiguration(configuration);
        assertThrows(AxonConfigurationException.class, builder::validate);
    }

    @Test
    void testBuildWithoutConsumerConfig() {
        KafkaTokenStore.Builder builder = KafkaTokenStore
                .builder()
                .serializer(serializer)
                .producerConfiguration(configuration);
        assertThrows(AxonConfigurationException.class, builder::validate);
    }

    @Test
    void testBuildWithoutProducerConfig() {
        KafkaTokenStore.Builder builder = KafkaTokenStore
                .builder()
                .serializer(serializer)
                .consumerConfiguration(configuration);
        assertThrows(AxonConfigurationException.class, builder::validate);
    }

    @Test
    void testBuildWithInvalidConsumerConfiguration() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        Map<String, Object> invalidConfiguration = new HashMap<>();
        assertThrows(AxonConfigurationException.class, () -> builder.consumerConfiguration(invalidConfiguration));
    }

    @Test
    void testBuildWithInvalidProducerConfiguration() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        Map<String, Object> invalidConfiguration = new HashMap<>();
        assertThrows(AxonConfigurationException.class, () -> builder.producerConfiguration(invalidConfiguration));
    }

    @Test
    void testBuildWithEmptyTopic() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.topic(""));
    }

    @Test
    void testBuildWithNullSerializer() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.serializer(null));
    }

    @Test
    void testBuildWithEmptyNodeId() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.nodeId(""));
    }

    @Test
    void testBuildWithNullExecutor() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.executor(null));
    }

    @Test
    void testBuildWithNullWriteTimeout() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.writeTimeout(null));
    }

    @Test
    void testBuildWithNullReadTimeout() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.readTimeOut(null));
    }

    @Test
    void testBuildWithNullOnShutdown() {
        KafkaTokenStore.Builder builder = KafkaTokenStore.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.onShutdown(null));
    }
}
