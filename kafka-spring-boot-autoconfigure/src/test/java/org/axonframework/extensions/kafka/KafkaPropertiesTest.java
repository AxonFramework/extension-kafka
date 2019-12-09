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

package org.axonframework.extensions.kafka;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify property injection in the {@link KafkaProperties} class through Spring Boot.
 *
 * @author Steven van Beelen
 */
@EnableAutoConfiguration
@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:application-map-style.properties")
class KafkaPropertiesTest {

    private static final String PROPERTY_KEY_ONE = "keyOne";
    private static final String PROPERTY_VALUE_ONE = "valueOne";
    private static final String PROPERTY_KEY_TWO = "keyTwo";
    private static final String PROPERTY_VALUE_TWO = "valueTwo";
    private static final String PROPERTY_KEY_THREE = "keyThree";
    private static final String PROPERTY_VALUE_THREE = "valueThree";
    private static final String PROPERTY_KEY_FOUR = "keyFour";
    private static final String PROPERTY_VALUE_FOUR = "valueFour";
    private static final String PROPERTY_KEY_FIVE = "keyFive";
    private static final String PROPERTY_VALUE_FIVE = "valueFive";

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private KafkaProperties testSubject;

    @Test
    void testPropertyMapIsInjectedAsExpected() {
        assertPropertyMap(testSubject.getProperties());
        assertPropertyMap(testSubject.getProducer().getProperties());
        assertPropertyMap(testSubject.getConsumer().getProperties());
    }

    private static void assertPropertyMap(Map<String, String> resultProperties) {
        assertTrue(resultProperties.containsKey(PROPERTY_KEY_ONE));
        assertEquals(PROPERTY_VALUE_ONE, resultProperties.get(PROPERTY_KEY_ONE));
        assertTrue(resultProperties.containsKey(PROPERTY_KEY_TWO));
        assertEquals(PROPERTY_VALUE_TWO, resultProperties.get(PROPERTY_KEY_TWO));
        assertTrue(resultProperties.containsKey(PROPERTY_KEY_THREE));
        assertEquals(PROPERTY_VALUE_THREE, resultProperties.get(PROPERTY_KEY_THREE));
        assertTrue(resultProperties.containsKey(PROPERTY_KEY_FOUR));
        assertEquals(PROPERTY_VALUE_FOUR, resultProperties.get(PROPERTY_KEY_FOUR));
        assertTrue(resultProperties.containsKey(PROPERTY_KEY_FIVE));
        assertEquals(PROPERTY_VALUE_FIVE, resultProperties.get(PROPERTY_KEY_FIVE));
    }
}