package org.axonframework.extensions.kafka.eventhandling.util;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class KafkaContainerClusterTest {

    @Container
    protected static final KafkaContainerCluster KAFKA_CLUSTER = new KafkaContainerCluster("5.4.3",
                                                                                           3,
                                                                                           1);
}
