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
import org.apache.kafka.common.TopicPartition;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test cases to verify the {@link TrackingRecordConverter} uses the provided {@link KafkaMessageConverter} upon the
 * {@link TrackingRecordConverter#convert(ConsumerRecords)} call and that the entries track progress in the {@link
 * TrackingToken}.
 *
 * @author Steven van Beelen
 */
class TrackingRecordConverterTest {

    private static final String TEST_TOPIC = "some-topic";
    private static final int TEST_PARTITION = 0;
    private static final TopicPartition TEST_TOPIC_PARTITION = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    private KafkaMessageConverter<String, String> messageConverter;

    private TrackingRecordConverter<String, String> testSubject;

    @BeforeEach
    void setUp() {
        //noinspection unchecked
        messageConverter = mock(KafkaMessageConverter.class);
        //noinspection unchecked
        when(messageConverter.readKafkaMessage(any())).thenAnswer(
                it -> Optional.of(asEventMessage(((ConsumerRecord<String, String>) it.getArgument(0)).value()))
        );

        testSubject = new TrackingRecordConverter<>(messageConverter, KafkaTrackingToken.emptyToken());
    }

    @Test
    void testProvidingNullTokenThrowsAssertionException() {
        //noinspection unchecked,rawtypes
        assertThrows(
                IllegalArgumentException.class,
                () -> new TrackingRecordConverter<>(mock(KafkaMessageConverter.class), null)
        );
    }

    @Test
    void testConverterConvertsRecordsAndTracksProgress() {
        int expectedNumberOfRecords = 42;
        long expectedOffset = expectedNumberOfRecords - 1;
        KafkaTrackingToken expectedCurrentToken =
                KafkaTrackingToken.newInstance(Collections.singletonMap(TEST_TOPIC_PARTITION, expectedOffset));

        ConsumerRecords<String, String> testRecords = buildConsumerRecords(expectedNumberOfRecords);

        List<KafkaEventMessage> result = testSubject.convert(testRecords);

        verify(messageConverter, times(expectedNumberOfRecords)).readKafkaMessage(any());
        assertEquals(expectedNumberOfRecords, result.size());

        KafkaEventMessage lastResult = result.get(result.size() - 1);
        assertEquals(TEST_PARTITION, lastResult.partition());
        assertEquals(expectedOffset, lastResult.offset());

        KafkaTrackingToken resultCurrentToken = testSubject.currentToken();
        assertEquals(expectedCurrentToken, resultCurrentToken);
    }

    @Test
    void testCurrentTokenReturnsTheGivenTokenIfNoConversionHasTakenPlace() {
        KafkaTrackingToken expectedToken = KafkaTrackingToken.emptyToken();
        TrackingRecordConverter<String, String> testSubject =
                new TrackingRecordConverter<>(messageConverter, expectedToken);

        KafkaTrackingToken resultToken = testSubject.currentToken();

        assertEquals(expectedToken, resultToken);
    }

    private static ConsumerRecords<String, String> buildConsumerRecords(int numberOfRecords) {
        List<ConsumerRecord<String, String>> consumerRecordList = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            consumerRecordList.add(buildRecord(i));
        }
        return new ConsumerRecords<>(Collections.singletonMap(
                new TopicPartition(TEST_TOPIC, TEST_PARTITION), consumerRecordList
        ));
    }

    private static ConsumerRecord<String, String> buildRecord(int offset) {
        return new ConsumerRecord<>(TEST_TOPIC, TEST_PARTITION, offset, "record-key", "record-value-" + offset);
    }
}
