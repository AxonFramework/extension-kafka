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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Defines a buffer that waits for the space to become non-empty when retrieving an element, and wait for space to
 * become available in the buffer when storing an element.
 *
 * @param <E> the type of the element contained in the buffer
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @since 4.0
 */
public interface Buffer<E> {

    /**
     * Inserts the provided element in this buffer, waiting for space to become available if the buffer is full.
     *
     * @param e the element to insert in this buffer
     * @throws InterruptedException if interrupted while waiting to put the element
     */
    void put(E e) throws InterruptedException;

    /**
     * Inserts the provided elements in this buffer, waiting for space to become available if the buffer is full.
     *
     * @param c the {@link Collection} of elements to be inserted in this buffer
     * @throws InterruptedException if interrupted while waiting to put any of the elements
     */
    void putAll(Collection<E> c) throws InterruptedException;

    /**
     * Retrieves and removes the first message of this buffer, waiting up to the specified wait time if necessary for a
     * message to become available.
     *
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit    a {@link TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return the first message of this buffer, or {@code null} if the specified waiting time elapses before a message
     * is available
     * @throws InterruptedException if interrupted while waiting to poll an element
     */
    E poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Retrieves and removes the first messages of this buffer, waiting if necessary until a message becomes available.
     *
     * @return the first message of this buffer
     * @throws InterruptedException if interrupted while waiting to take the first element
     */
    E take() throws InterruptedException;

    /**
     * Retrieves, but does not remove, the first message of this buffer, or returns {@code null} if this buffer is
     * empty.
     *
     * @return the first message in this buffer or {@code null} if the buffer is empty
     */
    E peek();

    /**
     * Returns the number of elements in this buffer.
     *
     * @return the number of elements in this buffer
     */
    int size();

    /**
     * Verify whether this buffer is empty or not.
     *
     * @return {@code true} if the buffer is empty and {@code false} if it isn't
     */
    boolean isEmpty();

    /**
     * Returns the number of additional elements that this buffer can ideally (in the absence of memory or resource
     * constraints) accept without blocking. This is always equal to the initial capacity of this buffer less the
     * current {@code size} of this buffer.
     * <p/>
     * Note that you <em>cannot</em> always tell if an attempt to insert an element will succeed by inspecting {@code
     * remainingCapacity} because it may be the case that another thread is about to insert or remove an element.
     *
     * @return the remaining capacity of this buffer
     */
    int remainingCapacity();

    /**
     * Removes all of the messages from this buffer.
     */
    void clear();
}
