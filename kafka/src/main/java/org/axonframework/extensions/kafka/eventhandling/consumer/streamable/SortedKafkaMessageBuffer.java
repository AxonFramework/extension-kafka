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

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.axonframework.common.Assert.isTrue;
import static org.axonframework.common.Assert.notNull;

/**
 * Thread safe buffer for storing incoming Kafka messages in sorted order defined via {@link Comparable}.
 *
 * @param <E> the type of the elements stored in this {@link Buffer} implementation
 * @author Nakul Mishra
 * @author Steven van Beelen
 * @author Gerard Klijs
 * @since 4.0
 */
public class SortedKafkaMessageBuffer<E extends Comparable<?> & KafkaRecordMetaData<?>> implements Buffer<E> {

    private static final Logger logger = LoggerFactory.getLogger(SortedKafkaMessageBuffer.class);

    private static final int DEFAULT_CAPACITY = 1_000;

    /**
     * Data structure used by this buffer.
     */
    private final ConcurrentSkipListSet<E> delegate;

    /**
     * Lock guarding all access to this buffer.
     */
    private final ReentrantLock lock;

    /**
     * Condition for waiting on {@link #take()}
     */
    private final Condition notEmpty;

    /**
     * Condition for waiting on {@link #put(Comparable)}
     */
    private final Condition notFull;

    /**
     * The max buffer size.
     */
    private final int capacity;

    /**
     * Number of messages in the buffer.
     */
    private int count;

    private final AtomicReference<RuntimeException> possibleException = new AtomicReference<>(null);


    /**
     * Create a default {@link SortedKafkaMessageBuffer} with capacity of {@code 1000}.
     */
    @SuppressWarnings("WeakerAccess")
    public SortedKafkaMessageBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Create a {@link SortedKafkaMessageBuffer} with the given max {@code capacity}.
     *
     * @param capacity the capacity of this buffer
     */
    public SortedKafkaMessageBuffer(int capacity) {
        isTrue(capacity > 0, () -> "The given capacity [" + capacity + "] may not be smaller than 0");
        this.delegate = new ConcurrentSkipListSet<>();
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
        this.notFull = lock.newCondition();
        this.capacity = capacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(E e) throws InterruptedException {
        notNull(e, () -> "Element may not be null");

        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            doPut(e);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Collection<E> c) throws InterruptedException {
        notNull(c, () -> "Element collection may not be null");

        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            for (E e : c) {
                doPut(e);
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    private void doPut(E e) throws InterruptedException {
        while (this.count == this.capacity) {
            this.notFull.await();
        }

        add(e);
        if (logger.isDebugEnabled()) {
            logger.debug("Buffer state after appending element [{}]", e);
            for (E message : delegate) {
                logger.debug(
                        "Partition:{}, Offset:{}, Timestamp:{}, Payload:{}",
                        message.partition(), message.offset(), message.timestamp(), message.value()
                );
            }
        }
    }

    /**
     * Inserts message, advances, and signals. This method should only be called when holding lock.
     */
    private void add(E x) {
        if (this.delegate.add(x)) {
            this.count++;
            this.notEmpty.signal();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();

        try {
            while (this.count == 0) {
                throwIfPresent();
                if (nanos <= 0) {
                    return null;
                }
                nanos = this.notEmpty.awaitNanos(nanos);
            }

            E removed = remove();
            if (logger.isDebugEnabled()) {
                logger.debug("Buffer state after removing element [{}]", removed);
                for (E message : delegate) {
                    logger.debug(
                            "Partition:{}, Offset:{}, Timestamp:{}, Payload:{}",
                            message.partition(), message.offset(), message.value(), message.timestamp()
                    );
                }
            }
            return removed;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E take() throws InterruptedException {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            while (this.count == 0) {
                throwIfPresent();
                this.notEmpty.await();
            }
            return remove();
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Extracts message and signals. This method should only be called when holding lock.
     */
    private E remove() {
        E x = this.delegate.pollFirst();
        if (x != null) {
            this.count--;
            this.notFull.signal();
        }
        return x;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E peek() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            if (this.count > 0) {
                return this.delegate.first();
            } else {
                throwIfPresent();
                return null;
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return this.count;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return this.count == 0;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int remainingCapacity() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return this.capacity - count;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            this.delegate.clear();
        } finally {
            reentrantLock.unlock();
        }
    }

    private void throwIfPresent() {
        RuntimeException e = possibleException.get();
        if (e != null) {
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SortedKafkaMessageBuffer:" + this.delegate;
    }

    /**
     * {@inheritDoc} Will throw the exception once the buffer is empty. Will throw the error either on {@link
     * #poll(long, TimeUnit)} or {@link #peek()} or {@link #take()} using {@link #throwIfPresent()}.
     */
    @Override
    public void setException(RuntimeException exception) {
        possibleException.set(exception);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            this.notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }
}
