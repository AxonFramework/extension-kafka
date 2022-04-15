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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link TokenUpdateDeserializer}
 *
 * @author Gerard Klijs
 */
@ExtendWith(MockitoExtension.class)
class TokenUpdateDeserializerTest {

    @Test
    void testBuildWithoutSerializer() {
        TokenUpdateSerializer serializer = new TokenUpdateSerializer();
        assertThrows(NotSupportedException.class, () -> serializer.serialize("topic", null));
    }
}
