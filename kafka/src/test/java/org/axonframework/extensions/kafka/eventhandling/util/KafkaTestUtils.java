package org.axonframework.extensions.kafka.eventhandling.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Kafka testing utilities. Copied what was needed either from spring-kafka or kafka. Original authors are kept.
 *
 * @author Gary Russell
 * @author Hugo Wood
 * @author Artem Bilan
 */
public class KafkaTestUtils {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTestUtils.class);

    /**
     * Poll the consumer for records.
     *
     * @param consumer the consumer.
     * @param <K>      the key type.
     * @param <V>      the value type.
     * @return the records.
     * @see #getRecords(Consumer, long)
     */
    public static <K, V> ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer) {
        return getRecords(consumer, 60000);
    }

    /**
     * Poll the consumer for records.
     *
     * @param consumer the consumer.
     * @param timeout  max time in milliseconds to wait for records; forwarded to {@link Consumer#poll(long)}.
     * @param <K>      the key type.
     * @param <V>      the value type.
     * @return the records.
     * @throws IllegalStateException if the poll returns null (since 2.3.4).
     * @since 2.0
     */
    public static <K, V> ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer, long timeout) {
        return getRecords(consumer, timeout, -1);
    }

    /**
     * Poll the consumer for records.
     *
     * @param consumer   the consumer.
     * @param timeout    max time in milliseconds to wait for records; forwarded to {@link Consumer#poll(long)}.
     * @param <K>        the key type.
     * @param <V>        the value type.
     * @param minRecords wait until the timeout or at least this number of receords are received.
     * @return the records.
     * @throws IllegalStateException if the poll returns null.
     * @since 2.4.2
     */
    public static <K, V> ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer, long timeout, int minRecords) {
        logger.debug("Polling...");
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
        long remaining = timeout;
        int count = 0;
        do {
            long t1 = System.currentTimeMillis();
            ConsumerRecords<K, V> received = consumer.poll(Duration.ofMillis(remaining));
            logger.debug("Received: " + received.count() + ", "
                                 + received.partitions().stream()
                                           .flatMap(p -> received.records(p).stream())
                                           // map to same format as send metadata toString()
                                           .map(r -> r.topic() + "-" + r.partition() + "@" + r.offset())
                                           .collect(Collectors.toList()));
            if (received == null) {
                throw new IllegalStateException("null received from consumer.poll()");
            }
            if (minRecords < 0) {
                return received;
            } else {
                count += received.count();
                received.partitions().forEach(tp -> {
                    List<ConsumerRecord<K, V>> recs = records.computeIfAbsent(tp, part -> new ArrayList<>());
                    recs.addAll(received.records(tp));
                });
                remaining -= System.currentTimeMillis() - t1;
            }
        }
        while (count < minRecords && remaining > 0);
        return new ConsumerRecords<>(records);
    }

    /**
     * Keep pooling from the {@code consumer} until we have at least the {@code numRecords} and return the records.
     *
     * @param consumer   the consumer
     * @param numRecords number of records wanted
     * @param waitTimeMs max wait time in milliseconds
     * @param <K>        key of the record
     * @param <V>        value of the record
     * @return list of records pulled from the consumer
     */
    public static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(
            Consumer<K, V> consumer,
            int numRecords,
            long waitTimeMs) {
        ArrayList<ConsumerRecord<K, V>> records = new ArrayList<>();
        Predicate<ConsumerRecords<K, V>> pollAction = cr -> {
            cr.forEach(records::add);
            return records.size() >= numRecords;
        };
        pollRecordsUntilTrue(consumer,
                             pollAction,
                             waitTimeMs,
                             String.format("Consumed %s records before timeout instead of the expected %s records",
                                           records.size(),
                                           numRecords)
        );
        return records;
    }

    private static <K, V> void pollRecordsUntilTrue(Consumer<K, V> consumer,
                                                    Predicate<ConsumerRecords<K, V>> pollAction,
                                                    long waitTimeMs, String msg) {
        waitUntilTrue(() -> {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
            return pollAction.test(records);
        }, msg, waitTimeMs, 100L);
    }

    /**
     * Wait until the given condition is true or throw an exception if the given wait time elapses.
     *
     * @param condition  condition to check
     * @param msg        error message
     * @param waitTimeMs maximum time to wait and retest the condition before failing the test
     * @param pause      delay between condition checks
     */
    @SuppressWarnings("squid:S2925") // need sleep to test
    private static void waitUntilTrue(Supplier<Boolean> condition, String msg, long waitTimeMs, long pause) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (condition.get()) {
                return;
            }
            if (System.currentTimeMillis() > startTime + waitTimeMs) {
                Assertions.fail(msg);
            }
            try {
                Thread.sleep(Long.min(waitTimeMs, pause));
            } catch (InterruptedException ex) {
                Assertions.fail("Interrupted by sleeping");
            }
        }
    }
}