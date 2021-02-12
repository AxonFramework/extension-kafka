package org.axonframework.extensions.kafka.eventhandling.util;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class KafkaContainerTest {

    @Container
    protected static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName
                                                                                       .parse("confluentinc/cp-kafka")
                                                                                       .withTag("5.4.3"));
}
