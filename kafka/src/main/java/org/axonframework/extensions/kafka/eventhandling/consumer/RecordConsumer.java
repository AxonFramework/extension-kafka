package org.axonframework.extensions.kafka.eventhandling.consumer;

import java.util.List;

/**
 * A functional interface towards consuming a {@link List} of records of type {@code E}. Provides added functionality
 * over the regular {@link java.util.function.Consumer} functional interface by specifying that it might throw an {@link
 * InterruptedException}.
 *
 * @param <E> the element type of the records to consume
 * @author Steven van Beelen
 * @since 4.0
 */
@FunctionalInterface
public interface RecordConsumer<E> {

    /**
     * Consume a {@link List} of records of type {@code E}.
     *
     * @param records the {@link List} of type {@code E} to consume
     * @throws InterruptedException if consumption is interrupted
     */
    void consume(List<E> records) throws InterruptedException;
}
