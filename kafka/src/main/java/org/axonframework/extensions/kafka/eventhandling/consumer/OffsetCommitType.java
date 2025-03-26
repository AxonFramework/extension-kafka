package org.axonframework.extensions.kafka.eventhandling.consumer;

/**
 * Enum to define how the consumer will handle committing offsets.
 */
public enum OffsetCommitType {
    AUTO,
    COMMIT_ASYNC,
    COMMIT_SYNC
}
