package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.eventhandling.EventMessage;

import java.util.Optional;
import java.util.function.Function;

/**
 * Interface to determine if a message should be published to Kafka, and if so to which topic. If the result from the
 * call is {@code Optional.empty()} is will not be published, else the result will be used for the topic.
 */
public interface TopicResolver extends Function<EventMessage<?>, Optional<String>> {

}
