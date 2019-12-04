/*
 * Copyright (c) 2010-2019. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.producer;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventMessageHandler;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An {@link EventMessageHandler} implementation responsible for sending {@link EventMessage} instances  to Kafka using
 * the provided {@link KafkaPublisher}.
 * <p>
 * For correct usage add this class to an {@link org.axonframework.eventhandling.EventProcessor} implementation using
 * the {@link KafkaEventPublisher#DEFAULT_PROCESSING_GROUP} constant as the processor and processing group name.
 *
 * @param <K> a generic type for the key of the {@link KafkaPublisher}
 * @param <V> a generic type for the value of the {@link KafkaPublisher}
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaEventPublisher<K, V> implements EventMessageHandler {

    /**
     * The default Kafka Event Handler processing group.
     * Not intended to be used by other Event Handling Components than {@link KafkaEventPublisher}.
     */
    public static final String DEFAULT_PROCESSING_GROUP = "__axon-kafka-event-publishing-group";
    private static final boolean DOES_NOT_SUPPORT_RESET = false;

    private final KafkaPublisher<K, V> kafkaPublisher;

    /**
     * Instantiate a Builder to be able to create a {@link KafkaEventPublisher}.
     * <p>
     * The {@link KafkaPublisher} is a <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link KafkaPublisher}
     * @param <V> a generic type for the value of the {@link KafkaPublisher}
     * @return a Builder to be able to create a {@link KafkaEventPublisher}
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Instantiate a {@link KafkaEventPublisher} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link KafkaPublisher} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is.
     *
     * @param builder the {@link Builder} used to instantiate a {@link KafkaEventPublisher} instance
     */
    protected KafkaEventPublisher(Builder<K, V> builder) {
        builder.validate();
        this.kafkaPublisher = builder.kafkaPublisher;
    }

    @Override
    public Object handle(EventMessage<?> event) {
        kafkaPublisher.send(event);
        return null;
    }

    @Override
    public boolean supportsReset() {
        return DOES_NOT_SUPPORT_RESET;
    }

    /**
     * Builder class to instantiate a {@link KafkaEventPublisher}.
     * <p>
     * The {@link KafkaPublisher} is a <b>hard requirements</b> and as such should be provided.
     *
     * @param <K> a generic type for the key of the {@link KafkaPublisher}
     * @param <V> a generic type for the value of the {@link KafkaPublisher}
     */
    public static class Builder<K, V> {

        private KafkaPublisher<K, V> kafkaPublisher;

        /**
         * Sets the {@link KafkaPublisher} to be used by this {@link EventMessageHandler} to publish
         * {@link EventMessage} on.
         *
         * @param kafkaPublisher the {@link KafkaPublisher} to be used by this {@link EventMessageHandler} to publish
         *                       {@link EventMessage} on
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<K, V> kafkaPublisher(KafkaPublisher<K, V> kafkaPublisher) {
            assertNonNull(kafkaPublisher, "KafkaPublisher may not be null");
            this.kafkaPublisher = kafkaPublisher;
            return this;
        }

        /**
         * Initializes a {@link KafkaEventPublisher} as specified through this Builder.
         *
         * @return a {@link KafkaEventPublisher} as specified through this Builder
         */
        public KafkaEventPublisher<K, V> build() {
            return new KafkaEventPublisher<>(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() {
            assertNonNull(kafkaPublisher, "The KafkaPublisher is a hard requirement and should be provided");
        }
    }
}
