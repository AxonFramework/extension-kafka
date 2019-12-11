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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

/**
 * An interface for messages originating from Kafka capable of providing information about their source.
 *
 * @param <V> the type of body used for this record
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public interface KafkaRecordMetaData<V> {

    /**
     * The partition from which this record is received.
     *
     * @return an {@code int} defining the partition from which this record is received
     */
    int partition();

    /**
     * The position of the record in the corresponding Kafka {@code partition}.
     *
     * @return a {@code long} defining the position of the record in the corresponding Kafka {@code partition}
     */
    long offset();

    /**
     * The timestamp of the record.
     *
     * @return a {@code long} defining the timestamp of this record
     */
    long timestamp();

    /**
     * The value of this record.
     *
     * @return the value of this record of type {@code V}
     */
    V value();
}
