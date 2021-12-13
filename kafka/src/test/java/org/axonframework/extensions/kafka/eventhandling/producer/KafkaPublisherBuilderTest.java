package org.axonframework.extensions.kafka.eventhandling.producer;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class KafkaPublisherBuilderTest {

    @Mock
    private ProducerFactory<String, byte[]> producerFactory;

    @Test
    void testKafkaPublisherInitialisationShouldNotThrowException() {
        assertDoesNotThrow(() -> KafkaPublisher.<String, byte[]>builder()
                                               .producerFactory(producerFactory)
                                               .build());
    }
}