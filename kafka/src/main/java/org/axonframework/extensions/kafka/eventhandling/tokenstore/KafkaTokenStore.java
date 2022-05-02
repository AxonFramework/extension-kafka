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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.BuilderUtils;
import org.axonframework.common.jdbc.ConnectionProvider;
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.AbstractTokenEntry;
import org.axonframework.eventhandling.tokenstore.GenericTokenEntry;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.eventhandling.tokenstore.jdbc.TokenSchema;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An implementation of TokenStore that allows you store and retrieve tracking tokens backed by a compacted Kafka
 * topic.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class KafkaTokenStore implements TokenStore {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTokenStore.class);
    private static final String DEFAULT_TOPIC = "__axon_token_store_updates";
    private static final String NOT_FOUND_MSG = "Unable to claim token '%s[%s]', It has not been initialized yet";

    private final String nodeId;
    private final TemporalAmount claimTimeout;
    private final TokenStoreState tokenStoreState;
    private final Serializer serializer;
    private final long readTimeOutMillis;

    /**
     * Instantiate a Builder to be able to create a {@link KafkaTokenStore}.
     * <p>
     * The {@code claimTimeout} is defaulted to a 10 seconds duration, {@code nodeId} is defaulted to the name of the
     * managed bean for the runtime system of the Java virtual machine and the {@code contentType} to a {@code byte[]}
     * {@link Class}. The {@link ConnectionProvider} and {@link Serializer} are <b>hard requirements</b> and as such
     * should be provided.
     *
     * @return a Builder to be able to create a {@link KafkaTokenStore}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@link KafkaTokenStore} based on the fields contained in the {@link KafkaTokenStore.Builder}.
     * <p>
     * Will assert that the {@link ConnectionProvider}, {@link Serializer}, {@link TokenSchema}, {@code claimTimeout},
     * {@code nodeId} and {@code contentType} are not {@code null}, and will throw an {@link AxonConfigurationException}
     * if any of them is {@code null}.
     *
     * @param builder the {@link KafkaTokenStore.Builder} used to instantiate a {@link KafkaTokenStore} instance
     */
    protected KafkaTokenStore(Builder builder) {
        builder.validate();
        this.nodeId = builder.nodeId;
        this.claimTimeout = builder.claimTimeout;
        this.tokenStoreState = new TokenStoreState(
                builder.topic,
                builder.claimTimeout,
                builder.consumerConfiguration,
                builder.producerConfiguration,
                builder.writeTimeout
        );
        this.serializer = builder.serializer;
        this.readTimeOutMillis = builder.readTimeOut.toMillis();
        start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeToken(@Nullable TrackingToken trackingToken, @Nonnull String processorName, int segment)
            throws UnableToClaimTokenException {
        Optional<TokenUpdate> current = tokenStoreState
                .getCurrent(processorName, segment)
                .map(this::updatableToken);
        if (!current.isPresent()) {
            throw new UnableToClaimTokenException(
                    String.format(NOT_FOUND_MSG, processorName, segment));
        }
        AbstractTokenEntry<byte[]> tokenEntry =
                new GenericTokenEntry<>(trackingToken, serializer, byte[].class, processorName, segment);
        if (!tokenEntry.claim(nodeId, claimTimeout)) {
            throw new UnableToClaimTokenException(
                    String.format("Unable to claim token '%s[%s]'. It is owned by '%s'",
                                  processorName,
                                  segment,
                                  tokenEntry.getOwner()));
        }
        TokenUpdate update = new TokenUpdate(tokenEntry, current.get().getSequenceNumber() + 1);
        send(update);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TrackingToken fetchToken(@Nonnull String processorName, int segment) throws UnableToClaimTokenException {
        Optional<TokenUpdate> current = tokenStoreState.getCurrent(processorName, segment);
        if (!current.isPresent()) {
            throw new UnableToClaimTokenException(
                    String.format(
                            "Unable to claim token '%s[%s]'. It has not been initialized yet",
                            processorName,
                            segment));
        }
        AbstractTokenEntry<byte[]> tokenEntry = current.get().toTokenEntry();
        if (!tokenEntry.claim(this.nodeId, this.claimTimeout)) {
            throw new UnableToClaimTokenException(
                    String.format("Unable to claim token '%s[%s]'. It is owned by '%s'",
                                  processorName,
                                  segment,
                                  tokenEntry.getOwner()));
        } else {
            TokenUpdate update = new TokenUpdate(tokenEntry, current.get().getSequenceNumber() + 1);
            send(update);
            return tokenEntry.getToken(serializer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requiresExplicitSegmentInitialization() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseClaim(@Nonnull String processorName, int segment) {
        Optional<TokenUpdate> current = tokenStoreState
                .getCurrent(processorName, segment)
                .map(this::updatableToken);
        if (!current.isPresent()) {
            throw new UnableToClaimTokenException(String.format(NOT_FOUND_MSG, processorName, segment));
        }
        send(tokenUpdateToReleaseUpdate(current.get()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteToken(@Nonnull String processorName, int segment) throws UnableToClaimTokenException {
        Optional<TokenUpdate> current = tokenStoreState
                .getCurrent(processorName, segment)
                .map(this::deletableToken);
        if (!current.isPresent()) {
            throw new UnableToClaimTokenException(String.format(NOT_FOUND_MSG, processorName, segment));
        }
        delete(current.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeTokenSegments(@Nonnull String processorName, int segmentCount)
            throws UnableToClaimTokenException {
        initializeTokenSegments(processorName, segmentCount, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeTokenSegments(@Nonnull String processorName, int segmentCount,
                                        @Nullable TrackingToken initialToken)
            throws UnableToClaimTokenException {
        int[] currentSegments = fetchSegments(processorName);
        if (currentSegments.length > 0) {
            throw new UnableToClaimTokenException(
                    String.format("Unable to initialize tokens for '%s', already %d segments exist",
                                  processorName, currentSegments.length)
            );
        }
        List<FutureWithContext> futures =
                IntStream.range(0, segmentCount)
                         .mapToObj(segment -> new GenericTokenEntry<>(initialToken,
                                                                      serializer,
                                                                      byte[].class,
                                                                      processorName,
                                                                      segment))
                         .map(tokenEntry -> new TokenUpdate(tokenEntry, 0))
                         .map(u -> {
                             Future<Boolean> future = tokenStoreState.send(u);
                             return new FutureWithContext(future, u.getProcessorName(), u.getSegment());
                         })
                         .collect(Collectors.toList());
        futures.forEach(this::handleFuture);
    }

    @Override
    public void initializeSegment(@Nullable TrackingToken token,
                                  @Nonnull String processorName,
                                  int segment) throws UnableToInitializeTokenException {
        Optional<TokenUpdate> current = tokenStoreState.getCurrent(processorName, segment);
        if (current.isPresent()) {
            throw new UnableToInitializeTokenException(
                    String.format("Unable to initialize token '%s[%s]', one already exist", processorName, segment)
            );
        }
        GenericTokenEntry<byte[]> tokenEntry = new GenericTokenEntry<>(token,
                                                                       serializer,
                                                                       byte[].class,
                                                                       processorName,
                                                                       segment);
        send(new TokenUpdate(tokenEntry, 0L));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] fetchSegments(@Nonnull String processorName) {
        return tokenStoreState.fetchSegments(processorName);
    }

    /**
     * Returns a List of known available {@code segments} for a given {@code processorName}. A segment is considered
     * available if it is not claimed by any other event processor.
     * <p>
     * The segments returned are segments for which a token has been stored previously and have not been claimed by
     * another processor. When the {@link TokenStore} is empty, an empty list is returned.
     * <p>
     * By default, if this method is not implemented, we will return all segments instead, whether they are available or
     * not.
     *
     * @param processorName the processor's name for which to fetch the segments
     * @return a List of available segment identifiers for the specified {@code processorName}
     */
    public List<Segment> fetchAvailableSegments(@Nonnull String processorName) {
        int[] allSegments = fetchSegments(processorName);
        return tokenStoreState.fetchAll(processorName)
                              .stream()
                              .filter(this::isAvailable)
                              .map(update -> Segment.computeSegment(update.getSegment(), allSegments))
                              .collect(Collectors.toList());
    }

    /**
     * Starts the token store state
     */
    public void start() {
        tokenStoreState.start();
    }

    /**
     * Closes the token store state
     */
    public void close() {
        tokenStoreState.close();
    }

    private static class FutureWithContext {

        final Future<Boolean> future;
        final String processorName;
        final int segment;

        private FutureWithContext(Future<Boolean> future, String processorName, int segment) {
            this.future = future;
            this.processorName = processorName;
            this.segment = segment;
        }
    }

    private void send(TokenUpdate update) {
        Future<Boolean> future = tokenStoreState.send(update);
        handleFuture(new FutureWithContext(future, update.getProcessorName(), update.getSegment()));
    }

    private void delete(TokenUpdate update) {
        Future<Boolean> future = tokenStoreState.send(new TokenUpdate(update, true));
        handleFuture(new FutureWithContext(future, update.getProcessorName(), update.getSegment()));
    }

    private void handleFuture(FutureWithContext fwc) {
        boolean result = false;
        try {
            result = fwc.future.get(readTimeOutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for send to '{}[{}]' to return", fwc.processorName, fwc.segment, e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("Error sending token '{}[{}]' to token state store", fwc.processorName, fwc.segment, e);
            throw new UnableToClaimTokenException(
                    String.format("Unable to process update '%s[%s]', error sending token",
                                  fwc.processorName,
                                  fwc.segment)
            );
        } catch (TimeoutException e) {
            throw new UnableToClaimTokenException(
                    String.format("Unable to process update '%s[%s]', timed out writing to store state",
                                  fwc.processorName,
                                  fwc.segment)
            );
        }
        if (!result) {
            throw new UnableToClaimTokenException(
                    String.format("Unable to process update '%s[%s]', concurrent write invalidated this one",
                                  fwc.processorName,
                                  fwc.segment)
            );
        }
    }

    private boolean isAvailable(TokenUpdate update) {
        try {
            updatableToken(update);
        } catch (UnableToClaimTokenException e) {
            logger.debug("token not available", e);
            return false;
        }
        return true;
    }

    private TokenUpdate updatableToken(TokenUpdate update) {
        if (update.getOwner() == null || update.getOwner().equals(nodeId)) {
            return update;
        }
        if (update.getTimestamp().isBefore(AbstractTokenEntry.clock.instant().minus(claimTimeout))) {
            return update;
        } else {
            throw new UnableToClaimTokenException(
                    String.format("Unable to claim token '%s[%s]', not the owner, and claim timeout is not expired yet",
                                  update.getProcessorName(), update.getSegment()));
        }
    }

    private TokenUpdate deletableToken(TokenUpdate update) {
        if (nodeId.equals(update.getOwner())) {
            return update;
        } else {
            throw new UnableToClaimTokenException(
                    String.format("Unable to remove token '%s[%s]'. It is not owned by %s",
                                  update.getProcessorName(),
                                  update.getSegment(),
                                  nodeId));
        }
    }

    private TokenUpdate tokenUpdateToReleaseUpdate(TokenUpdate update) {
        AbstractTokenEntry<byte[]> tokenEntry = update.toTokenEntry();
        tokenEntry.releaseClaim(update.getOwner());
        return new TokenUpdate(tokenEntry, update.getSequenceNumber() + 1);
    }

    /**
     * Builder class to instantiate a {@link KafkaTokenStore}.
     * <p>
     * The {@code topic} is defaulted to __axon_token_store_updates. The {@code claimTimeout} is defaulted to a 10
     * seconds duration (by using {@link Duration#ofSeconds(long)}, {@code nodeId} is defaulted to the {@code
     * ManagementFactory#getRuntimeMXBean#getName} output, the {@code readTimeOut} to a 5 seconds duration (by using
     * {@link Duration#ofSeconds(long)}, and the {@code writeTimeout} to a 3 seconds duration (by using {@link
     * Duration#ofSeconds(long)}. The {@code consumerConfiguration}, {@code producerConfiguration} and {@link
     * Serializer} are <b>hard requirements</b> and as such should be provided. For the {@code consumerConfiguration}
     * and {@code producerConfiguration} the most important property is the {@code bootstrap.servers} config, as well as
     * any security config needed.
     */
    public static class Builder {

        private String topic = DEFAULT_TOPIC;
        private Serializer serializer;
        private TemporalAmount claimTimeout = Duration.ofSeconds(10);
        private String nodeId = ManagementFactory.getRuntimeMXBean().getName();
        private Map<String, Object> consumerConfiguration;
        private Map<String, Object> producerConfiguration;
        private Duration readTimeOut = Duration.ofSeconds(5L);
        private Duration writeTimeout = Duration.ofSeconds(3L);

        /**
         * Sets the {@code topic} specifying the topic used for the token claim updates. Defaults to
         * __axon_token_store_updates. Please make sure that either the topic doesn't exist yet, and can be created be
         * the consumer, or the topic is configured correctly. It should be a compacted topic, and it might have
         * multiple partition, but one should be more than enough for the expected traffic. The {@code
         * min.compaction.lag.ms} of the topic should be at least double the {@code claimTimeout}. This is because we
         * consider the first message with the same sequence number as the 'correct' message, instead of Kafka, that
         * might delete a message when there is a newer message using the same key. To be sure the topic is cleaned up
         * in time, independent of the default configured values, the {@code max.compaction.lag.ms} can be set to a
         * value about 4 times the {@code min.compaction.lag.ms} value.
         *
         * @param topic the topic used for token claim updates
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder topic(String topic) {
            BuilderUtils.assertNonEmpty(topic, "The topic may not be null");
            this.topic = topic;
            return this;
        }

        /**
         * Sets the {@link Serializer} used to de-/serialize {@link TrackingToken}s with.
         *
         * @param serializer a {@link Serializer} used to de-/serialize {@link TrackingToken}s with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Sets the {@code claimTimeout} specifying the amount of time this process will wait after which this process
         * will force a claim of a {@link TrackingToken}. Thus, if a claim has not been updated for the given {@code
         * claimTimeout}, this process will 'steal' the claim. Defaults to a duration of 10 seconds.
         *
         * @param claimTimeout a timeout specifying the time after which this process will force a claim
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder claimTimeout(TemporalAmount claimTimeout) {
            assertNonNull(claimTimeout, "The claim timeout may not be null");
            this.claimTimeout = claimTimeout;
            return this;
        }

        /**
         * Sets the {@code nodeId} to identify ownership of the tokens. Defaults to the name of the managed bean for the
         * runtime system of the Java virtual machine.
         *
         * @param nodeId the id as a {@link String} to identify ownership of the tokens
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder nodeId(String nodeId) {
            BuilderUtils.assertNonEmpty(nodeId, "The nodeId may not be null or empty");
            this.nodeId = nodeId;
            return this;
        }

        /**
         * Sets the {@code consumerConfiguration} specifying configuration for reading token updates.
         *
         * @param consumerConfiguration a timeout specifying the time after which this process will force a claim
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder consumerConfiguration(Map<String, Object> consumerConfiguration) {
            assertMinimalValidClientConfiguration(consumerConfiguration,
                                                  "The consumer configuration may not be null, and needs to contain a 'bootstrap.servers' value");
            this.consumerConfiguration = setConsumerConfig(consumerConfiguration);
            return this;
        }

        /**
         * Sets the {@code producerConfiguration} specifying configuration for reading token updates.
         *
         * @param producerConfiguration a timeout specifying the time after which this process will force a claim
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder producerConfiguration(Map<String, Object> producerConfiguration) {
            assertMinimalValidClientConfiguration(producerConfiguration,
                                                  "The consumer configuration may not be null, and needs to contain a 'bootstrap.servers' value");
            this.producerConfiguration = setProducerConfig(producerConfiguration);
            return this;
        }

        /**
         * Sets the {@code readTimeOut} specifying configuration for reading token updates. Defaults to a duration of 5
         * seconds.
         *
         * @param readTimeOut the duration until a read by the Kafka consumer is considered as failure
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder readTimeOut(Duration readTimeOut) {
            BuilderUtils.assertNonNull(readTimeOut, "The readTimeOut may not be null");
            this.readTimeOut = readTimeOut;
            return this;
        }

        /**
         * Sets the {@code writeTimeout} specifying configuration for reading token updates. Defaults to a duration of 3
         * seconds.
         *
         * @param writeTimeout the duration until an update by the Kafka producer is considered as failure
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder writeTimeout(Duration writeTimeout) {
            BuilderUtils.assertNonNull(writeTimeout, "The readTimeOut may not be null");
            this.writeTimeout = writeTimeout;
            return this;
        }

        /**
         * Initializes a {@link KafkaTokenStore} as specified through this Builder.
         *
         * @return a {@link KafkaTokenStore} as specified through this Builder
         */
        public KafkaTokenStore build() {
            return new KafkaTokenStore(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            BuilderUtils.assertNonEmpty(topic, "The topic is a hard requirement and should be provided");
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
            BuilderUtils.assertNonEmpty(nodeId, "The nodeId is a hard requirement and should be provided");
            assertMinimalValidClientConfiguration(consumerConfiguration,
                                                  "Consumer configuration is a hard requirement and should at least contain a 'bootstrap.servers' value");
            assertMinimalValidClientConfiguration(producerConfiguration,
                                                  "Producer configuration is a hard requirement and should at least contain a 'bootstrap.servers' value");
        }

        private Map<String, Object> setConsumerConfig(Map<String, Object> configuration) {
            Map<String, Object> result = new HashMap<>(configuration);
            result.remove(ConsumerConfig.GROUP_ID_CONFIG);
            result.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            result.put(ConsumerConfig.GROUP_ID_CONFIG, "earliest");
            result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TokenUpdateDeserializer.class);
            return result;
        }

        private Map<String, Object> setProducerConfig(Map<String, Object> configuration) {
            Map<String, Object> result = new HashMap<>(configuration);
            result.put(ProducerConfig.LINGER_MS_CONFIG, 0);
            result.put(ProducerConfig.ACKS_CONFIG, "1");
            result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TokenUpdateSerializer.class);
            return result;
        }

        private void assertMinimalValidClientConfiguration(Map<String, Object> configuration, String exceptionMessage) {
            assertNonNull(configuration, exceptionMessage);
            Object bootstrapServer = configuration.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
            assertNonNull(bootstrapServer, exceptionMessage);
        }
    }
}
