package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private KafkaMessageConverter<String, String> messageConverter;

    private TrackingRecordConverter<String, String> testSubject;

    @BeforeEach
    void setUp() {
        //noinspection unchecked
        messageConverter = mock(KafkaMessageConverter.class);
        when(messageConverter.readKafkaMessage(any()))
                .thenAnswer(it -> Optional.of(asEventMessage(((ConsumerRecord) it.getArgument(0)).value())));

        testSubject = new TrackingRecordConverter<>(messageConverter, KafkaTrackingToken.emptyToken());
    }

    @Test
    void testProvidingNullTokenThrowsAssertionException() {
        //noinspection unchecked
        assertThrows(
                IllegalArgumentException.class,
                () -> new TrackingRecordConverter(mock(KafkaMessageConverter.class), null)
        );
    }

    @Test
    void testConverterConvertsRecordsAndTracksProgress() {
        int expectedNumberOfRecords = 42;
        int expectedOffset = expectedNumberOfRecords - 1;

        ConsumerRecords<String, String> testRecords = buildConsumerRecords(expectedNumberOfRecords);

        List<KafkaEventMessage> result = testSubject.convert(testRecords);

        verify(messageConverter, times(expectedNumberOfRecords)).readKafkaMessage(any());
        assertEquals(expectedNumberOfRecords, result.size());

        KafkaEventMessage lastResult = result.get(result.size() - 1);
        assertEquals(TEST_PARTITION, lastResult.partition());
        assertEquals(expectedOffset, lastResult.offset());

        TrackingToken lastResultToken = lastResult.value().trackingToken();
        assertTrue(lastResultToken instanceof KafkaTrackingToken);
        Map<Integer, Long> partitionPositions = ((KafkaTrackingToken) lastResultToken).partitionPositions();
        assertEquals(1, partitionPositions.size());
        assertEquals(expectedOffset, partitionPositions.get(TEST_PARTITION));
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
