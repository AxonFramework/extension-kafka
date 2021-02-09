package org.axonframework.extensions.kafka.eventhandling;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractKafkaIntegrationBaseTest {

    protected static final KafkaContainer KAFKA_CONTAINER;

    static {
        KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3")); // TODO extract version
        KAFKA_CONTAINER.start();

        try {
            Thread.sleep(30000); // todo Use Testcontainer's waiting mechanism
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Kafka container started on " + KAFKA_CONTAINER.getBootstrapServers()); // todo Remove
    }

}
