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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.RecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.axonframework.common.Assert.nonNull;

/**
 * {@link RecordConverter} instances which keeps track of the converted {@link ConsumerRecords} through a {@link
 * KafkaTrackingToken}. Consequently it converts the ConsumerRecords in to {@link KafkaEventMessage} instances.
 *
 * @param <K> the key of the Kafka {@link ConsumerRecords} to be converted
 * @param <V> the value type of Kafka {@link ConsumerRecords} to be converted
 * @author Steven van Beelen
 * @since 4.0
 */
public class TrackingRecordConverter<K, V> implements RecordConverter<K, V, KafkaEventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(TrackingRecordConverter.class);

    private final KafkaMessageConverter<K, V> messageConverter;
    private KafkaTrackingToken currentToken;

    /**
     * Instantiates a {@link TrackingRecordConverter}, using the {@link KafkaMessageConverter} to convert {@link
     * ConsumerRecord} instances in to an {@link org.axonframework.eventhandling.EventMessage} instances. As it
     * traverses the {@link ConsumerRecords} it will advance the provided {@code token}. An {@link
     * IllegalArgumentException} will be thrown if the provided {@code token} is {@code null}.
     *
     * @param messageConverter the {@link KafkaMessageConverter} used to convert a {@link ConsumerRecord} in to an
     *                         {@link org.axonframework.eventhandling.EventMessage}
     * @param token            the {@link KafkaTrackingToken} to advance for every fetched {@link ConsumerRecord}
     */
    public TrackingRecordConverter(KafkaMessageConverter<K, V> messageConverter, KafkaTrackingToken token) {
        this.messageConverter = messageConverter;
        this.currentToken = nonNull(token, () -> "Token may not be null");
    }

    /**
     * {@inheritDoc}
     * <p>
     * {@code E} is defined as a {@link KafkaEventMessage} for this implementation. Every {@link ConsumerRecord} will
     * advance the defined {@code token}'s position further with the ConsumerRecord's {@link ConsumerRecord#partition()}
     * and {@link ConsumerRecord#offset()}.
     */
    @Override
    public List<KafkaEventMessage> convert(ConsumerRecords<K, V> records) {
        List<KafkaEventMessage> eventMessages = new ArrayList<>(records.count());
        for (ConsumerRecord<K, V> record : records) {
            messageConverter.readKafkaMessage(record).ifPresent(eventMessage -> {
                KafkaTrackingToken nextToken =
                        currentToken.advancedTo(record.topic(), record.partition(), record.offset());
                logger.debug("Advancing token from [{}] to [{}]", currentToken, nextToken);

                currentToken = nextToken;
                eventMessages.add(KafkaEventMessage.from(eventMessage, record, currentToken));
            });
        }
        return eventMessages;
    }

    /**
     * Return the current state of the {@link KafkaTrackingToken} this converter updates
     *
     * @return the current state of the {@link KafkaTrackingToken} this converter updates
     */
    public KafkaTrackingToken currentToken() {
        return currentToken;
    }
}
