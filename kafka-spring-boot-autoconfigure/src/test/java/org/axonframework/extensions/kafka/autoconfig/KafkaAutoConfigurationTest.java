/*
 * Copyright (c) 2010-2019. Axon Framework
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

package org.axonframework.extensions.kafka.autoconfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.config.EventProcessingModule;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.ConfirmationMode;
import org.axonframework.extensions.kafka.eventhandling.producer.DefaultProducerFactory;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.ProducerFactory;
import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.monitoring.NoOpMessageMonitor;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.axonframework.spring.config.AxonConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher.DEFAULT_PROCESSING_GROUP;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link KafkaAutoConfiguration}, verifying the minimal set of requirements and full fledged adjustments
 * from a producer and a consumer perspective.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 */
class KafkaAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));

    @Test
    void testAutoConfigurationWithMinimalRequiredProperties() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo"
                          ).run(context -> {
            // Required bean assertions
            assertNotNull(context.getBeanNamesForType(KafkaMessageConverter.class));
            assertNotNull(context.getBeanNamesForType(ProducerFactory.class));
            assertNotNull(context.getBeanNamesForType(KafkaPublisher.class));
            assertNotNull(context.getBeanNamesForType(KafkaEventPublisher.class));
            assertNotNull(context.getBeanNamesForType(ConsumerFactory.class));
            assertNotNull(context.getBeanNamesForType(Fetcher.class));
            assertNotNull(context.getBeanNamesForType(StreamableKafkaMessageSource.class));

            // Producer assertions
            DefaultProducerFactory<?, ?> producerFactory =
                    ((DefaultProducerFactory<?, ?>) context.getBean(DefaultProducerFactory.class));
            Map<String, Object> producerConfigs =
                    ((DefaultProducerFactory<?, ?>) context.getBean(DefaultProducerFactory.class))
                            .configurationProperties();
            KafkaProperties kafkaProperties = context.getBean(KafkaProperties.class);

            assertEquals(ConfirmationMode.TRANSACTIONAL, producerFactory.confirmationMode());
            assertEquals("foo", producerFactory.transactionIdPrefix());
            assertEquals(StringSerializer.class, producerConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            assertEquals(ByteArraySerializer.class, producerConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            assertEquals(
                    KafkaProperties.EventProcessorMode.SUBSCRIBING,
                    kafkaProperties.getProducer().getEventProcessorMode()
            );

            // Consumer assertions
            Map<String, Object> consumerConfigs =
                    ((DefaultConsumerFactory<?, ?>) context.getBean(DefaultConsumerFactory.class))
                            .configurationProperties();

            assertEquals(
                    Collections.singletonList("localhost:9092"),
                    consumerConfigs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
            );
            assertNull(consumerConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
            assertNull(consumerConfigs.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
            assertEquals(
                    StringDeserializer.class, consumerConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
            );
            assertEquals(
                    ByteArrayDeserializer.class, consumerConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
            );
        });
    }

    @Test
    void testConsumerPropertiesAreAdjustedAsExpected() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  "axon.kafka.default-topic=testTopic",
                                  // Overrides 'axon.kafka.bootstrap-servers'
                                  "axon.kafka.bootstrap-servers=foo:1234",
                                  "axon.kafka.properties.foo=bar",
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.properties.baz=qux",
                                  "axon.kafka.properties.foo.bar.baz=qux.fiz.buz",
                                  "axon.kafka.ssl.key-password=p1",
                                  "axon.kafka.ssl.keystore-location=classpath:ksLoc",
                                  "axon.kafka.ssl.keystore-password=p2",
                                  "axon.kafka.ssl.truststore-location=classpath:tsLoc",
                                  "axon.kafka.ssl.truststore-password=p3",
                                  "axon.kafka.consumer.auto-commit-interval=123",
                                  "axon.kafka.consumer.max-poll-records=42",
                                  "axon.kafka.consumer.auto-offset-reset=earliest",
                                  "axon.kafka.consumer.client-id=some-client-id",
                                  "axon.kafka.consumer.enable-auto-commit=false",
                                  "axon.kafka.consumer.fetch-max-wait=456",
                                  "axon.kafka.consumer.properties.fiz.buz=fix.fox",
                                  "axon.kafka.consumer.fetch-min-size=789",
                                  "axon.kafka.consumer.group-id=bar",
                                  "axon.kafka.consumer.heartbeat-interval=234",
                                  "axon.kafka.consumer.key-deserializer = org.apache.kafka.common.serialization.LongDeserializer",
                                  "axon.kafka.consumer.value-deserializer = org.apache.kafka.common.serialization.IntegerDeserializer"
                          ).run(context -> {
            // Required bean assertions
            assertNotNull(context.getBeanNamesForType(KafkaMessageConverter.class));
            assertNotNull(context.getBeanNamesForType(ConsumerFactory.class));
            assertNotNull(context.getBeanNamesForType(Fetcher.class));
            assertNotNull(context.getBeanNamesForType(StreamableKafkaMessageSource.class));

            // Consumer assertions
            DefaultConsumerFactory<?, ?> consumerFactory = context.getBean(DefaultConsumerFactory.class);
            Map<String, Object> configs = consumerFactory.configurationProperties();

            assertEquals(
                    Collections.singletonList("foo:1234"),
                    configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
            ); // Assert override
            assertEquals("p1", configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
            assertTrue(
                    ((String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).contains(File.separator + "ksLoc")
            );
            assertEquals("p2", configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
            assertTrue(
                    ((String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).contains(File.separator + "tsLoc")
            );
            assertEquals("p3", configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            assertEquals("some-client-id", configs.get(ConsumerConfig.CLIENT_ID_CONFIG));
            assertEquals(Boolean.FALSE, configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
            assertEquals(123, configs.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            assertEquals("earliest", configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
            assertEquals(456, configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
            assertEquals(789, configs.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
            assertEquals(234, configs.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
            assertEquals(LongDeserializer.class, configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
            assertEquals(IntegerDeserializer.class, configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            assertEquals(42, configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
            assertEquals("bar", configs.get("foo"));
            assertEquals("qux", configs.get("baz"));
            assertEquals("qux.fiz.buz", configs.get("foo.bar.baz"));
            assertEquals("fix.fox", configs.get("fiz.buz"));
        });
    }

    @Test
    void testProducerPropertiesAreAdjustedAsExpected() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  "axon.kafka.clientId=cid",
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo",
                                  "axon.kafka.properties.foo.bar.baz=qux.fiz.buz",
                                  "axon.kafka.producer.acks=all",
                                  "axon.kafka.producer.batch-size=20",
                                  // Overrides "axon.kafka.producer.bootstrap-servers"
                                  "axon.kafka.producer.bootstrap-servers=bar:1234",
                                  "axon.kafka.producer.buffer-memory=12345",
                                  "axon.kafka.producer.compression-type=gzip",
                                  "axon.kafka.producer.key-serializer=org.apache.kafka.common.serialization.LongSerializer",
                                  "axon.kafka.producer.retries=2",
                                  "axon.kafka.producer.properties.fiz.buz=fix.fox",
                                  "axon.kafka.producer.ssl.key-password=p4",
                                  "axon.kafka.producer.ssl.keystore-location=classpath:ksLocP",
                                  "axon.kafka.producer.ssl.keystore-password=p5",
                                  "axon.kafka.producer.ssl.truststore-location=classpath:tsLocP",
                                  "axon.kafka.producer.ssl.truststore-password=p6",
                                  "axon.kafka.producer.value-serializer=org.apache.kafka.common.serialization.IntegerSerializer"
                          ).run(context -> {
            DefaultProducerFactory<?, ?> producerFactory = context.getBean(DefaultProducerFactory.class);
            Map<String, Object> configs = producerFactory.configurationProperties();

            // Producer assertions
            assertEquals("cid", configs.get(ProducerConfig.CLIENT_ID_CONFIG));
            assertEquals("all", configs.get(ProducerConfig.ACKS_CONFIG));
            assertEquals(20, configs.get(ProducerConfig.BATCH_SIZE_CONFIG));
            assertEquals(
                    Collections.singletonList("bar:1234"),
                    configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            ); // Assert override
            assertEquals(12345L, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
            assertEquals("gzip", configs.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            assertEquals(LongSerializer.class, configs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            assertEquals("p4", configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
            assertTrue(
                    ((String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).contains(File.separator + "ksLocP")
            );
            assertEquals("p5", configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
            assertTrue(
                    ((String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
                            .contains(File.separator + "tsLocP")
            );
            assertEquals("p6", configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
            assertEquals(2, configs.get(ProducerConfig.RETRIES_CONFIG));
            assertEquals(IntegerSerializer.class, configs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            assertEquals("qux.fiz.buz", configs.get("foo.bar.baz"));
            assertEquals("fix.fox", configs.get("fiz.buz"));
        });
    }

    @Test
    void testKafkaPropertiesTrackingProducerMode() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  // Minimal Required Properties
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo",
                                  // Event Producing Mode
                                  "axon.kafka.producer.event-processor-mode=TRACKING"
                          ).run(context -> {
            KafkaProperties kafkaProperties = context.getBean(KafkaProperties.class);
            assertEquals(
                    KafkaProperties.EventProcessorMode.TRACKING,
                    kafkaProperties.getProducer().getEventProcessorMode()
            );
            assertNotNull(context.getBean(KafkaEventPublisher.class));

            EventProcessingConfigurer eventProcessingConfigurer = context.getBean(EventProcessingConfigurer.class);
            verify(eventProcessingConfigurer).registerEventHandler(any());
            verify(eventProcessingConfigurer)
                    .registerListenerInvocationErrorHandler(eq(DEFAULT_PROCESSING_GROUP), any());
            verify(eventProcessingConfigurer).assignHandlerTypesMatching(eq(DEFAULT_PROCESSING_GROUP), any());
            verify(eventProcessingConfigurer).registerTrackingEventProcessor(DEFAULT_PROCESSING_GROUP);
        });
    }

    @Test
    void testKafkaPropertiesSubscribingProducerMode() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  // Minimal Required Properties
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo",
                                  // Event Producing Mode
                                  "axon.kafka.producer.event-processor-mode=SUBSCRIBING"
                          ).run(context -> {
            KafkaProperties kafkaProperties = context.getBean(KafkaProperties.class);
            assertEquals(
                    KafkaProperties.EventProcessorMode.SUBSCRIBING,
                    kafkaProperties.getProducer().getEventProcessorMode()
            );
            assertNotNull(context.getBean(KafkaEventPublisher.class));

            EventProcessingConfigurer eventProcessingConfigurer = context.getBean(EventProcessingConfigurer.class);
            verify(eventProcessingConfigurer).registerEventHandler(any());
            verify(eventProcessingConfigurer)
                    .registerListenerInvocationErrorHandler(eq(DEFAULT_PROCESSING_GROUP), any());
            verify(eventProcessingConfigurer).assignHandlerTypesMatching(eq(DEFAULT_PROCESSING_GROUP), any());
            verify(eventProcessingConfigurer).registerSubscribingEventProcessor(DEFAULT_PROCESSING_GROUP);
        });
    }

    @Test
    void testKafkaPropertiesTrackingConsumerMode() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  // Minimal Required Properties
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo",
                                  // Event Consumption Mode
                                  "axon.kafka.consumer.event-processor-mode=TRACKING"
                          ).run(context -> {
            KafkaProperties kafkaProperties = context.getBean(KafkaProperties.class);
            assertEquals(
                    KafkaProperties.EventProcessorMode.TRACKING,
                    kafkaProperties.getConsumer().getEventProcessorMode()
            );
            assertNotNull(context.getBean(StreamableKafkaMessageSource.class));
        });
    }

    @Test
    void testKafkaPropertiesSubscribingConsumerMode() {
        this.contextRunner.withUserConfiguration(TestConfiguration.class)
                          .withPropertyValues(
                                  // Minimal Required Properties
                                  "axon.kafka.default-topic=testTopic",
                                  "axon.kafka.producer.transaction-id-prefix=foo",
                                  // Event Consumption Mode
                                  "axon.kafka.consumer.event-processor-mode=SUBSCRIBING"
                          ).run(context -> {
            KafkaProperties kafkaProperties = context.getBean(KafkaProperties.class);
            assertEquals(
                    KafkaProperties.EventProcessorMode.SUBSCRIBING,
                    kafkaProperties.getConsumer().getEventProcessorMode()
            );
            assertThrows(
                    NoSuchBeanDefinitionException.class, () -> context.getBean(StreamableKafkaMessageSource.class)
            );
        });
    }

    @Configuration
    protected static class TestConfiguration {

        @Bean
        public Serializer eventSerializer() {
            return XStreamSerializer.builder().build();
        }

        @Bean
        public EventBus eventBus() {
            return mock(EventBus.class);
        }

        @Bean
        public AxonConfiguration axonConfiguration() {
            AxonConfiguration mock = mock(AxonConfiguration.class);
            //noinspection unchecked,rawtypes
            when(mock.messageMonitor(any(), any())).thenReturn((MessageMonitor) NoOpMessageMonitor.instance());
            return mock;
        }

        @Bean
        public EventProcessingModule eventProcessingConfigurer() {
            return spy(new EventProcessingModule());
        }
    }
}
