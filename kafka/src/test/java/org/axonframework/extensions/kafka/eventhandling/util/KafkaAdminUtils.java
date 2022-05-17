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

package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test utility for Kafka admin operations.
 *
 * @author Stefan Andjelkovic
 * @author Lucas Campos
 */
public abstract class KafkaAdminUtils {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private KafkaAdminUtils() {
        // Utils
    }

    /**
     * Method responsible for creating the {@code topics} on the provided {@code bootstrapServer}.
     *
     * @param bootstrapServer the kafka address
     * @param topics          a list of topics to be created
     */
    public static void createTopics(String bootstrapServer, String... topics) {
        createTopics(bootstrapServer, 3, topics);
    }

    /**
     * Method responsible for creating the {@code topics} on the provided {@code bootstrapServer}.
     *
     * @param bootstrapServer the kafka address
     * @param retries         the number of retries if there is an error
     * @param topics          a list of topics to be created
     */
    public static void createTopics(String bootstrapServer, int retries, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(bootstrapServer))) {
            CreateTopicsResult topicsCreationResult = adminClient.createTopics(topics(topics));
            topicsCreationResult.values().values()
                                .forEach(KafkaAdminUtils::waitForCompletion);
            Arrays.stream(topics).forEach(topic -> logger.info("Completed topic creation: {}", topic));
        } catch (Exception e) {
            logger.warn("Encountered an exception while creating topics [{}] for [{}].", topics, bootstrapServer, e);
            if (retries > 0) {
                int retriesLeft = retries - 1;
                logger.info("Retrying topic creation, retries left: {}", retriesLeft);
                createTopics(bootstrapServer, retriesLeft, topics);
            } else {
                throw e;
            }
        }
    }

    /**
     * Method responsible for creating partitions on given {@code bootstrapServer} and {@code topics}.
     *
     * @param bootstrapServer the kafka address
     * @param nrPartitions    the number os partitions to be created on each topic
     * @param topics          a list of topics to be created
     */
    public static void createPartitions(String bootstrapServer, Integer nrPartitions, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(bootstrapServer))) {
            CreatePartitionsResult partitionCreationResult =
                    adminClient.createPartitions(partitions(nrPartitions, topics));
            partitionCreationResult.values().values()
                                   .forEach(KafkaAdminUtils::waitForCompletion);
            Arrays.stream(topics).forEach(topic -> logger
                    .info("Completed {} partition creation on topic: {}", nrPartitions, topic));
        }
    }

    /**
     * Method responsible for deleting the {@code topics} on the provided {@code bootstrapServer}.
     *
     * @param bootstrapServer the kafka address
     * @param topics          a list of topics to be deleted
     */
    public static void deleteTopics(String bootstrapServer, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(bootstrapServer))) {
            DeleteTopicsResult topicsDeletionResult = adminClient.deleteTopics(Arrays.asList(topics));
            waitForCompletion(topicsDeletionResult.all());
            Arrays.stream(topics).forEach(topic -> logger.info("Completed topic deletion: {}", topic));
        }
    }

    /**
     * Method responsible for listing the {@code topics} on the provided {@code bootstrapServer}.
     *
     * @param bootstrapServer the kafka address
     */
    public static Set<String> listTopics(String bootstrapServer) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(bootstrapServer))) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            return waitForCompletion(listTopicsResult.names());
        }
    }

    private static <T> T waitForCompletion(KafkaFuture<T> kafkaFuture) {
        try {
            return kafkaFuture.get(25, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<NewTopic> topics(String... topics) {
        return Arrays.stream(topics)
                     .map(KafkaTopicConfiguration::new)
                     .map(topic -> new NewTopic(topic.getName(),
                                                topic.getNumPartitions(),
                                                topic.getReplicationFactor()))
                     .collect(Collectors.toList());
    }

    private static Map<String, NewPartitions> partitions(Integer nrPartitions, String... topics) {
        return Stream.of(topics)
                     .collect(Collectors.toMap(Function.identity(), ignored -> NewPartitions.increaseTo(nrPartitions)));
    }

    private static Map<String, Object> minimalAdminConfig(String bootstrapServer) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return configs;
    }

    static class KafkaTopicConfiguration {

        private final String name;
        private final int numPartitions;
        private final short replicationFactor;

        public KafkaTopicConfiguration(String name, int numPartitions, short replicationFactor) {
            this.name = name;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }

        public KafkaTopicConfiguration(String name) {
            this(name, 1, (short) 1);
        }

        public String getName() {
            return name;
        }

        public int getNumPartitions() {
            return numPartitions;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }
    }
}

