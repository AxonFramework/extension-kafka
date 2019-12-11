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

package org.axonframework.extensions.kafka.configuration;

import org.axonframework.config.Configuration;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import static org.mockito.Mockito.*;

/**
 * Test classes verifying registered message sources are started and closed through the {@link
 * org.axonframework.config.ModuleConfiguration} API.
 *
 * @author Steven van Beelen
 */
@ExtendWith(MockitoExtension.class)
class KafkaMessageSourceConfigurerTest {

    private KafkaMessageSourceConfigurer testSubject = new KafkaMessageSourceConfigurer();

    @Test
    void testStartInitiatesRegisteredSubscribableSources(
            @Mock Configuration configuration,
            @Mock SubscribableKafkaMessageSource<?, ?> sourceOne,
            @Mock SubscribableKafkaMessageSource<?, ?> sourceTwo
    ) {
        testSubject.registerSubscribableSource(conf -> sourceOne);
        testSubject.registerSubscribableSource(conf -> sourceTwo);

        testSubject.initialize(configuration);
        testSubject.start();

        verify(sourceOne).start();
        verify(sourceTwo).start();
    }

    @Test
    void testShutdownClosesRegisteredSubscribableSources(
            @Mock Configuration configuration,
            @Mock SubscribableKafkaMessageSource<?, ?> sourceOne,
            @Mock SubscribableKafkaMessageSource<?, ?> sourceTwo
    ) {
        testSubject.registerSubscribableSource(conf -> sourceOne);
        testSubject.registerSubscribableSource(conf -> sourceTwo);

        testSubject.initialize(configuration);
        testSubject.shutdown();

        verify(sourceOne).close();
        verify(sourceTwo).close();
    }
}