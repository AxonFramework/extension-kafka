/*
 * Copyright (c) 2010-2023. Axon Framework
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

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers. Copied from
 * https://github.com/testcontainers/testcontainers-java/blob/master/examples/kafka-cluster/src/test/java/com/example/kafkacluster/KafkaContainerCluster.java
 * <p>
 * Since it was copied from the link above, we decided to not make any major changes to it and use it as is.
 */
public class KafkaContainerCluster implements Startable {

    private final int brokersNum;
    private final Network network;
    private final GenericContainer<?> zookeeper;
    private final Collection<KafkaContainer> brokers;

    /**
     * Default constructor for building a kafka cluster to be used for tests.
     *
     * @param confluentPlatformVersion version of the kafka image
     * @param brokersNum               number of brokers
     * @param internalTopicsRf         replication factor
     */
    public KafkaContainerCluster(String confluentPlatformVersion, int brokersNum, int internalTopicsRf) {
        if (brokersNum < 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
        if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
            throw new IllegalArgumentException(
                    "internalTopicsRf '" + internalTopicsRf + "' must be less than brokersNum and greater than 0");
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        this.zookeeper = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper")
                                                               .withTag(confluentPlatformVersion))
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT));

        this.brokers = IntStream
                .range(0, this.brokersNum)
                .mapToObj(brokerNum -> new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")
                                                                         .withTag(confluentPlatformVersion))
                        .withNetwork(this.network)
                        .withNetworkAliases("broker-" + brokerNum)
                        .dependsOn(this.zookeeper)
                        .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT)
                        .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + ""))
                .collect(Collectors.toList());
    }

    /**
     * Return the bootstrap server location (url) used for creating topics, partitions, etc.
     *
     * @return location of the servers
     */
    public String getBootstrapServers() {
        return brokers.stream()
                      .map(KafkaContainer::getBootstrapServers)
                      .collect(Collectors.joining(","));
    }

    private Stream<GenericContainer<?>> allContainers() {
        return Stream.concat(
                this.brokers.stream(),
                Stream.of(this.zookeeper)
        );
    }

    /**
     * Used by test container lifecycle itself.
     */
    @Override
    public void start() {
        Stream<Startable> startables = this.brokers.stream().map(Startable.class::cast);
        try {
            Startables.deepStart(startables).get(60, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
        Unreliables.retryUntilTrue(60, TimeUnit.SECONDS, () -> {
            Container.ExecResult result = this.zookeeper.execInContainer(
                    "sh", "-c",
                    "zookeeper-shell zookeeper:" + KafkaContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
            );
            String brokers = result.getStdout();

            return brokers != null && brokers.split(",").length == this.brokersNum;
        });
    }

    /**
     * Used by test container lifecycle itself.
     */
    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }
}