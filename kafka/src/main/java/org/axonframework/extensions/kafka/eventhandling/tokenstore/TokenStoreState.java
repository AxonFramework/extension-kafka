/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.eventhandling.tokenstore;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.axonframework.common.AxonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Contains the logic for maintaining a consistent state for the token store.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
class TokenStoreState {

    private static final Logger logger = LoggerFactory.getLogger(TokenStoreState.class);
    private static final String SEQUENCE_ERROR_FORMAT = "%d is not after %d for processor %s with segment %d";
    private static final String KEY_FORMAT = "%s::%d";
    private final Map<String, Map<Integer, TokenUpdate>> state = new ConcurrentHashMap<>();
    private final Map<UUID, CompletableFuture<Boolean>> writeResult = new ConcurrentHashMap<>();
    private final Executor executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Boolean>> isReady = new AtomicReference<>(new CompletableFuture<>());
    private final AtomicReference<Producer<String, TokenUpdate>> producer = new AtomicReference<>(null);

    private final String topic;
    private final TemporalAmount claimTimeout;
    private final Map<String, Object> consumerConfiguration;
    private final Map<String, Object> producerConfiguration;
    private final long writeTimeOutMillis;

    /**
     * initializes the TokenStoreState
     *
     * @param topic                 the name of the topic, if it doesn't exist it will be created
     * @param claimTimeout          the claim timeout of the store, used to set the topic config
     * @param consumerConfiguration used to configure the Kafka consumer
     * @param producerConfiguration used to configure the Kafka producer
     */
    TokenStoreState(String topic,
                    TemporalAmount claimTimeout,
                    Map<String, Object> consumerConfiguration,
                    Map<String, Object> producerConfiguration,
                    Duration writeTimeOut
    ) {
        this.topic = topic;
        this.claimTimeout = claimTimeout;
        this.consumerConfiguration = consumerConfiguration;
        this.producerConfiguration = producerConfiguration;
        this.writeTimeOutMillis = writeTimeOut.toMillis();
    }

    Future<Boolean> send(TokenUpdate update) {
        String key = String.format(KEY_FORMAT, update.getProcessorName(), update.getSegment());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        writeResult.put(update.getId(), future);
        try {
            producer.updateAndGet(p -> {
                if (p == null) {
                    return new KafkaProducer<>(producerConfiguration);
                }
                return p;
            }).send(new ProducerRecord<>(topic, key, update)).get(writeTimeOutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            writeResult.remove(update.getId());
            future.complete(false);
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            logger.warn("error sending token update '{}[{}]' to Kafka",
                        update.getProcessorName(),
                        update.getSegment(),
                        e);
            writeResult.remove(update.getId());
            future.complete(false);
        }
        return future;
    }

    Optional<TokenUpdate> getCurrent(String processorName, int segment) {
        waitTillReady();
        return Optional.ofNullable(state.get(processorName)).map(s -> s.get(segment));
    }

    int[] fetchSegments(String processorName) {
        waitTillReady();
        return Optional.ofNullable(state.get(processorName)).map(this::toPrimitiveIntArray).orElse(new int[0]);
    }

    Collection<TokenUpdate> fetchAll(String processorName) {
        waitTillReady();
        return Optional.ofNullable(state.get(processorName)).map(Map::values).orElse(Collections.emptyList());
    }

    void start() {
        if (isRunning.compareAndSet(false, true)) {
            executor.execute(this::startConsumer);
            producer.set(new KafkaProducer<>(producerConfiguration));
        }
    }

    void close() {
        isRunning.set(false);
        producer.updateAndGet(p -> {
            if (p != null) {
                p.close();
            }
            return null;
        });
    }

    private int[] toPrimitiveIntArray(Map<Integer, TokenUpdate> segments) {
        Iterator<Integer> integers = segments.keySet().iterator();
        int[] ints = new int[segments.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = integers.next();
        }
        return ints;
    }

    private void waitTillReady() {
        try {
            boolean result = isReady.get().get();
            if (!result) {
                waitTillReady();
            }
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting until token store state was ready", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("Error waiting till token store state was ready", e);
            throw new TokenStoreInitializationException("Token store is not ready yet");
        }
    }

    private void startConsumer() {
        try (Consumer<String, TokenUpdate> consumer = new KafkaConsumer<>(consumerConfiguration)) {
            Map<String, List<PartitionInfo>> topicInfo = consumer.listTopics();
            if (!topicInfo.containsKey(topic)) {
                createTopic();
                topicInfo = consumer.listTopics();
            }
            List<TopicPartition> partitions = topicInfo
                    .get(topic)
                    .stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toList());
            consumer.assign(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            while (isRunning.get()) {
                ConsumerRecords<String, TokenUpdate> records = consumer.poll(Duration.ofMillis(100L));
                logger.debug("received: {} records", records.count());
                records.forEach(this::update);
                if (!endOffsets.isEmpty()) {
                    List<TopicPartition> toRemove = new ArrayList<>();
                    endOffsets.forEach((tp, o) -> {
                        long current = consumer.position(tp);
                        if (current >= o) {
                            toRemove.add(tp);
                        }
                    });
                    toRemove.forEach(endOffsets::remove);
                    if (endOffsets.isEmpty()) {
                        isReady.get().complete(true);
                    }
                }
            }
        } catch (Exception e) {
            isReady.getAndUpdate(c -> {
                c.complete(false);
                return new CompletableFuture<>();
            });
            logger.warn("Error consuming to update Kafka token store", e);
            if (isRunning.get()) {
                logger.info("Restarting consumer, the model is kept, so errors updating state are expected");
                executor.execute(this::startConsumer);
            }
        }
    }

    /**
     * See the {@link KafkaTokenStore.Builder#topic(String) topic} method for the reasoning behind the configuration.
     */
    private void createTopic() {
        try (AdminClient client = AdminClient.create(consumerConfiguration)) {
            short replicationFactor = (short) Math.min(3, client.describeCluster().nodes().get().size());
            NewTopic newTopic = new NewTopic(topic, 1, replicationFactor);
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            topicConfig.put(
                    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
                    String.valueOf((2 * Duration.from(claimTimeout).getSeconds()))
            );
            topicConfig.put(
                    TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
                    String.valueOf((8 * Duration.from(claimTimeout).getSeconds()))
            );
            newTopic.configs(topicConfig);
            client.createTopics(Collections.singletonList(newTopic));
        } catch (ExecutionException | InterruptedException e) {
            logger.warn("error creating topic for Kafka token store", e);
            Thread.currentThread().interrupt();
        }
    }

    private void update(ConsumerRecord<String, TokenUpdate> consumerRecord) {
        boolean result;
        TokenUpdate update;
        if (consumerRecord.value() != null) {
            update = consumerRecord.value();
            result = updateAddition(update);
        } else {
            update = new TokenUpdate(consumerRecord.headers(), null);
            result = updateDeletion(update);
        }
        writeResult.computeIfPresent(
                update.getId(),
                (k, f) -> {
                    f.complete(result);
                    return null;
                });
    }

    private boolean updateAddition(TokenUpdate tokenUpdate) {
        try {
            updateState(tokenUpdate);
            return true;
        } catch (TokenStoreUpdateException e) {
            logger.info("failed to update state for '{}[{}]'",
                        tokenUpdate.getProcessorName(),
                        tokenUpdate.getSegment(),
                        e);
            return false;
        }
    }

    private boolean updateDeletion(TokenUpdate tokenUpdate) {
        final AtomicBoolean result = new AtomicBoolean(false);
        state.computeIfPresent(tokenUpdate.getProcessorName(), (k, v) -> {
            result.set(v.remove(tokenUpdate.getSegment()) != null);
            return v;
        });
        return result.get();
    }

    private void updateState(TokenUpdate tokenUpdate) {
        Map<Integer, TokenUpdate> innerMap = state.computeIfAbsent(
                tokenUpdate.getProcessorName(),
                x -> new ConcurrentHashMap<>());
        innerMap.merge(tokenUpdate.getSegment(), tokenUpdate, this::mergeFunction);
    }

    private TokenUpdate mergeFunction(TokenUpdate oldUpdate, TokenUpdate newUpdate) {
        if (newUpdate.getSequenceNumber() > oldUpdate.getSequenceNumber()) {
            return newUpdate;
        } else {
            throw new TokenStoreUpdateException(String.format(SEQUENCE_ERROR_FORMAT,
                                                              newUpdate.getSequenceNumber(),
                                                              oldUpdate.getSequenceNumber(),
                                                              newUpdate.getProcessorName(),
                                                              newUpdate.getSegment()));
        }
    }

    /**
     * Exception thrown when failing to update the token store.
     *
     * @author Gerard Klijs
     * @since 4.6.0
     */
    private static class TokenStoreUpdateException extends AxonException {

        /**
         * Initializes the exception using the given {@code message}.
         *
         * @param message The message describing the exception
         */
        private TokenStoreUpdateException(String message) {
            super(message);
        }
    }
}
