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

package org.axonframework.extensions.kafka.eventhandling.cloudevent;

import org.axonframework.common.AxonException;

/**
 * Exception thrown when failing to add metadata to a cloud event.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class InvalidMetaDataException extends AxonException {

    /**
     * Initializes the exception using the given {@code message}.
     *
     * @param message The message describing the exception.
     */
    public InvalidMetaDataException(String message) {
        super(message);
    }
}
