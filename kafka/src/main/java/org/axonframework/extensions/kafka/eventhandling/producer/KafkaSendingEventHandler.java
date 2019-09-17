package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
@ProcessingGroup("axon.kafka.event")
public class KafkaSendingEventHandler {

    private final KafkaPublisher kafkaPublisher;

    public KafkaSendingEventHandler(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @EventHandler
    public <T> void handle(EventMessage<T> message) {
        kafkaPublisher.send(Collections.singletonList(message));
    }
}
