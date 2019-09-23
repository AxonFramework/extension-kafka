package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;

@ProcessingGroup(KafkaSendingEventHandler.GROUP)
public class KafkaSendingEventHandler {

    public static final String GROUP = "axon.kafka.event";

    private final KafkaPublisher kafkaPublisher;

    public KafkaSendingEventHandler(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @EventHandler
    public <T> void handle(EventMessage<T> message) {
        kafkaPublisher.send(message);
    }
}
