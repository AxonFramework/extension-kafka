package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.testcontainers.containers.KafkaContainer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class KafkaAdminUtils {

    public static void createTopics(KafkaContainer kafkaContainer, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(kafkaContainer))) {
            CreateTopicsResult topicsCreationResult = adminClient.createTopics(topics(topics));
            topicsCreationResult.values().values()
                                .forEach(KafkaAdminUtils::waitForCompletion);
            System.out.println("Completed topic creation");
        }
    }

    public static void createPartitions(KafkaContainer kafkaContainer, Integer nrPartitions, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(kafkaContainer))) {
            CreatePartitionsResult partitionCreationResult = adminClient.createPartitions(partitions(nrPartitions,
                                                                                                     topics));
            partitionCreationResult.values().values()
                                   .forEach(KafkaAdminUtils::waitForCompletion);
            System.out.println("Completed partition creation");
        }
    }

    public static void deleteTopics(KafkaContainer kafkaContainer, String... topics) {
        try (AdminClient adminClient = AdminClient.create(minimalAdminConfig(kafkaContainer))) {
            DeleteTopicsResult topicsDeletionResult = adminClient.deleteTopics(Arrays.asList(topics));
            topicsDeletionResult.values().values()
                                .forEach(KafkaAdminUtils::waitForCompletion);
            System.out.println("Completed topic deletion");
        }
    }

    private static void waitForCompletion(KafkaFuture<Void> kafkaFuture) {
        try {
            kafkaFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public static List<NewTopic> topics(String... topics) {
        return Arrays.stream(topics)
                     .map(topic -> new KafkaAdminUtils.KafkaTopicConfiguration(topic))
                     .map(topic -> new NewTopic(topic.getName(),
                                                topic.getNumPartitions(),
                                                topic.getReplicationFactor()))
                     .collect(Collectors.toList());
    }

    public static Map<String, NewPartitions> partitions(Integer nrPartitions, String... topics) {
        return Stream.of(topics)
                     .collect(Collectors.toMap(Function.identity(), ignored -> NewPartitions.increaseTo(nrPartitions)));
    }

    public static Map<String, Object> minimalAdminConfig(KafkaContainer kafkaContainer) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    kafkaContainer.getBootstrapServers()); // kafka instances
        return configs;
    }

    public static class KafkaTopicConfiguration {

        private final String name;
        private final int numPartitions;
        private final short replicationFactor;

        public KafkaTopicConfiguration(String name, int numPartitions, short replicationFactor) {
            this.name = name;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }

        public KafkaTopicConfiguration(String name, int numPartitions) {
            this(name, numPartitions, (short) 1);
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

