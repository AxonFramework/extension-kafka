package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/**
 * A functional interface towards converting the {@link org.apache.kafka.clients.consumer.ConsumerRecord} instances in
 * to a {@link List} of {@code E}.
 *
 * @param <E> the element type each {@link org.apache.kafka.clients.consumer.ConsumerRecord} instance is converted in to
 * @param <K> the key of the {@link ConsumerRecords}
 * @param <V> the value type of {@link ConsumerRecords}
 * @author Steven van Beelen
 * @since 4.0
 */
@FunctionalInterface
public interface RecordConverter<E, K, V> {

    /**
     * Covert the provided {@code records} in to a {@link List} of elements of type {@code E}.
     *
     * @param records a {@link ConsumerRecords} instance to convert in to a {@link List} of {@code E}
     * @return the {@link List} of elements of type {@code E}
     */
    List<E> convert(ConsumerRecords<K, V> records);
}
