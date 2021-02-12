package org.axonframework.extensions.kafka.eventhandling.util;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A cluster of kafkas using test container.
 */
@Testcontainers
public class KafkaContainerClusterTest {

    @Container
    protected static final KafkaContainerCluster KAFKA_CLUSTER = new KafkaContainerCluster("5.4.3", 3, 1);

    protected static String getBootstrapServers() {
        return KAFKA_CLUSTER.getBootstrapServers();
    }
}
