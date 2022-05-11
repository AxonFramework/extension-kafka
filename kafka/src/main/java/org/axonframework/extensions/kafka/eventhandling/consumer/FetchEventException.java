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

package org.axonframework.extensions.kafka.eventhandling.consumer;

import org.axonframework.common.AxonException;

/**
 * Exception thrown when there is an error while either fetching records from Kafka, or processing them.
 *
 * @author Gerard Klijs
 * @since 4.5.4
 */
public class FetchEventException extends AxonException {

    /**
     * Creates a new {@link FetchEventException}.
     *
     * @param message some info about the exception
     */
    public FetchEventException(String message) {
        super(message);
    }

    /**
     * Creates a new {@link FetchEventException}
     *
     * @param message some info about the exception
     * @param cause   the {@link Throwable} that is the cause of the exception
     */
    public FetchEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
