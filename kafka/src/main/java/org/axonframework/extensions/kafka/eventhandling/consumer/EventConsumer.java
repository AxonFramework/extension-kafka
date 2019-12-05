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

package org.axonframework.extensions.kafka.eventhandling.consumer;

import java.util.List;

/**
 * A functional interface towards consuming a {@link List} of records of type {@code E}. Provides added functionality
 * over the regular {@link java.util.function.Consumer} functional interface by specifying that it might throw an {@link
 * InterruptedException}.
 *
 * @param <E> the element type of the records to consume
 * @author Steven van Beelen
 * @since 4.0
 */
@FunctionalInterface
public interface EventConsumer<E> {

    /**
     * Consume a {@link List} of records of type {@code E}.
     *
     * @param records the {@link List} of type {@code E} to consume
     * @throws InterruptedException if consumption is interrupted
     */
    void consume(List<E> records) throws InterruptedException;
}
