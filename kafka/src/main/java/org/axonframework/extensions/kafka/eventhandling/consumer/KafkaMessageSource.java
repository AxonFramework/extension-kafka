/*
 * Copyright (c) 2010-2018. Axon Framework
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

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.stream.BlockingStream;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.messaging.StreamableMessageSource;

import java.util.Objects;

import static org.axonframework.common.Assert.isTrue;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * Implementation of the {@link StreamableMessageSource} that reads messages from a Kafka topic using the provided
 * {@link Fetcher}.
 *
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public class KafkaMessageSource implements StreamableMessageSource<TrackedEventMessage<?>> {

    private final Fetcher fetcher;
    private final String groupId;

    /**
     * Instantiate a Builder to be able to create a {@link KafkaMessageSource}.
     * <p>
     * The {@link Fetcher} and {@code groupId} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create an {@link KafkaMessageSource}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@link KafkaMessageSource} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Fetcher} is not {@code null} and that the {@code groupId} is a non-empty {@link
     * String}. An {@link AxonConfigurationException} is thrown if either of both is not the case.
     *
     * @param builder the {@link Builder} used to instantiate a {@link KafkaMessageSource} instance
     */
    @SuppressWarnings("WeakerAccess")
    protected KafkaMessageSource(Builder builder) {
        builder.validate();
        this.fetcher = builder.fetcher;
        this.groupId = builder.groupId;
    }

    @SuppressWarnings("ConstantConditions") // Verified TrackingToken type through `Assert.isTrue` operation
    @Override
    public BlockingStream<TrackedEventMessage<?>> openStream(TrackingToken trackingToken) {
        isTrue(
                trackingToken == null || trackingToken instanceof KafkaTrackingToken,
                () -> "Incompatible token type provided."
        );

        return fetcher.start((KafkaTrackingToken) trackingToken, groupId);
    }

    /**
     * Builder class to instantiate a {@link KafkaMessageSource}.
     * <p>
     * The {@link Fetcher} and {@code groupId} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private Fetcher fetcher;
        private String groupId;

        /**
         * Sets the {@link Fetcher} used to fetch a {@link BlockingStream} of {@link TrackedEventMessage} instances.
         *
         * @param fetcher the {@link Fetcher} used to fetch a {@link BlockingStream} of {@link TrackedEventMessage}
         *                instances
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder fetcher(Fetcher fetcher) {
            assertNonNull(fetcher, "Fetcher may not be null");
            this.fetcher = fetcher;
            return this;
        }

        /**
         * Sets the Consumer {@code groupId} from which a {@link BlockingStream} should retrieve its events from
         *
         * @param groupId a {@link String} defining the Consumer Group id from which a {@link BlockingStream} should
         *                retrieve its events from
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder groupId(String groupId) {
            assertThat(
                    groupId, name -> Objects.nonNull(name) && !"".equals(name),
                    "The groupId may not be null or empty"
            );
            this.groupId = groupId;
            return this;
        }

        /**
         * Initializes a {@link KafkaMessageSource} as specified through this Builder.
         *
         * @return a {@link KafkaMessageSource} as specified through this Builder
         */
        public KafkaMessageSource build() {
            return new KafkaMessageSource(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @SuppressWarnings("WeakerAccess")
        protected void validate() throws AxonConfigurationException {
            assertNonNull(fetcher, "The Fetcher is a hard requirement and should be provided");
            assertThat(groupId,
                       name -> Objects.nonNull(name) && !"".equals(name),
                       "The groupId may not be null or empty");
        }
    }
}
