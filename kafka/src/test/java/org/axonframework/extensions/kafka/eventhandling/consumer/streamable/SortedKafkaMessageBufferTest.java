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

import org.axonframework.extensions.kafka.eventhandling.consumer.FetchEventException;
import org.junit.jupiter.api.*;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.axonframework.eventhandling.EventUtils.asTrackedEventMessage;
import static org.axonframework.eventhandling.GenericEventMessage.asEventMessage;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SortedKafkaMessageBuffer}.
 *
 * @author Nakul Mishra
 * @author Gerard Klijs
 */
public class SortedKafkaMessageBufferTest extends JSR166TestCase {

    private static SortedKafkaMessageBuffer<KafkaEventMessage> populatedBuffer(int size,
                                                                               int minCapacity,
                                                                               int maxCapacity) {
        SortedKafkaMessageBuffer<KafkaEventMessage> buff = null;
        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            int capacity = rnd.nextInt(minCapacity, maxCapacity + 1);
            buff = new SortedKafkaMessageBuffer<>(capacity);
            assertTrue(buff.isEmpty());
            // shuffle circular array elements so they wrap
            {
                int n = rnd.nextInt(capacity);
                for (int i = 0; i < n; i++) {
                    buff.put(message(42, 42, 42, "42"));
                }
                for (int i = 0; i < n; i++) {
                    buff.poll(1, TimeUnit.NANOSECONDS);
                }
            }
            for (int i = 0; i < size; i++) {
                buff.put(message(i, i, i, "ma"));
            }
            assertEquals(size == 0, buff.isEmpty());
            assertEquals(capacity - size, buff.remainingCapacity());
            assertEquals(size, buff.size());
            if (size > 0) {
                assertNotNull(buff.peek());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return buff;
    }

    private static void concurrent(@SuppressWarnings("SameParameterValue") int noOfThreads,
                                   Runnable task) throws Throwable {
        CyclicBarrier barrier = new CyclicBarrier(noOfThreads + 1);
        ExecutorService pool = Executors.newFixedThreadPool(noOfThreads);
        Collection<Future<Void>> futures = new LinkedList<>();
        for (int i = 0; i < noOfThreads; i++) {
            futures.add(pool.submit(() -> {
                barrier.await();
                task.run();
                return null;
            }));
        }
        barrier.await();
        pool.shutdown();

        for (Future<Void> future : futures) {
            try {
                future.get(1, TimeUnit.MINUTES);
            } catch (ExecutionException e) {
                if (isAssertionError(e)) {
                    throw e.getCause();
                }
                throw e.getCause();
            } catch (TimeoutException e) {
                fail("Updates took to long.");
            }
        }
        //Test should complete in max 1 Minute.
        Assertions.assertTrue(pool.awaitTermination(1, TimeUnit.MINUTES),
                              "Excepted to finish in a minute but took longer");
    }

    private static boolean isAssertionError(ExecutionException e) {
        return e.getCause() instanceof AssertionError;
    }

    private static KafkaEventMessage message(int partition, int offset, int timestamp, String value) {
        return new KafkaEventMessage(asTrackedEventMessage(asEventMessage(value), null), partition, offset, timestamp);
    }

    public void testCreateBufferWithNonPositiveCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SortedKafkaMessageBuffer<>(-1));
    }

    public void testPutInvalidMessageInABuffer() {
        SortedKafkaMessageBuffer<KafkaEventMessage> messageBuffer = new SortedKafkaMessageBuffer<>();
        assertThrows(IllegalArgumentException.class, () -> messageBuffer.put(null));
    }

    /**
     * A new buffer has the indicated capacity
     */
    public void testCreateBuffer() {
        assertEquals(SIZE, new SortedKafkaMessageBuffer<>(SIZE).remainingCapacity());
    }

    /**
     * Queue transitions from empty to full when elements added
     */
    public void testEmptyFull() {
        SortedKafkaMessageBuffer<KafkaEventMessage> buff = populatedBuffer(0, 2, 2);
        assertTrue(buff.isEmpty());
        assertEquals(2, buff.remainingCapacity());
        try {
            buff.put(message(0, 0, 0, "m0"));
            buff.put(message(0, 0, 0, "m1"));
            assertEquals(1, buff.remainingCapacity());
            assertFalse(buff.isEmpty());
            buff.put(message(0, 1, 1, "m1"));
            assertFalse(buff.isEmpty());
            assertEquals(0, buff.remainingCapacity());
        } catch (InterruptedException failure) {
            failure.printStackTrace();
        }
    }

    public void testIsEmpty() {
        SortedKafkaMessageBuffer<? extends Comparable<?>> buff = new SortedKafkaMessageBuffer<>();
        assertTrue(buff.isEmpty());
        assertEquals(0, buff.size());
    }

    /**
     * remainingCapacity decreases on add, increases on remove
     */
    public void testRemainingCapacity() throws InterruptedException {
        int size = ThreadLocalRandom.current().nextInt(1, SIZE);
        SortedKafkaMessageBuffer<KafkaEventMessage> buff = populatedBuffer(size, size, 2 * size);
        int spare = buff.remainingCapacity();
        int capacity = spare + size;
        for (int i = 0; i < size; i++) {
            assertEquals(spare + i, buff.remainingCapacity());
            assertEquals(capacity, buff.size() + buff.remainingCapacity());
            assertNotNull(buff.take());
        }
        for (int i = 0; i < size; i++) {
            assertEquals(capacity - i, buff.remainingCapacity());
            assertEquals(capacity, buff.size() + buff.remainingCapacity());
            buff.put(message(0, i, i, "a"));
            assertNotNull(buff.peek());
        }
    }

    /**
     * Put blocks interruptible if full
     */
    public void testBlockingPut() {
        final SortedKafkaMessageBuffer<KafkaEventMessage> buff = new SortedKafkaMessageBuffer<>(SIZE);
        final CountDownLatch pleaseInterrupt = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i) {
                    buff.put(message(i, i, i, "m"));
                }
                assertEquals(SIZE, buff.size());
                assertEquals(0, buff.remainingCapacity());

                Thread.currentThread().interrupt();
                assertThrows(InterruptedException.class, () -> buff.put(message(99, 99, 99, "m")));

                assertFalse(Thread.interrupted());

                pleaseInterrupt.countDown();
                assertThrows(InterruptedException.class, () -> buff.put(message(99, 99, 99, "m")));
                assertFalse(Thread.interrupted());
            }
        });

        await(pleaseInterrupt);
        assertThreadBlocks(t, Thread.State.WAITING);
        t.interrupt();
        awaitTermination(t);
        assertEquals(SIZE, buff.size());
        assertEquals(0, buff.remainingCapacity());
    }

    /**
     * Checks that thread eventually enters the expected blocked thread state.
     */
    @SuppressWarnings("SameParameterValue")
    private void assertThreadBlocks(Thread thread, Thread.State expected) {
        // Always sleep at least 1 ms, with high probability avoiding transitory states
        for (long retries = LONG_DELAY_MS * 3 / 4; retries-- > 0; ) {
            try {
                delay(1);
            } catch (InterruptedException fail) {
                fail("Unexpected InterruptedException");
            }
            Thread.State s = thread.getState();
            if (s == expected) {
                return;
            } else if (s == Thread.State.TERMINATED) {
                fail("Unexpected thread termination");
            }
        }
        fail("timed out waiting for thread to enter thread state " + expected);
    }

    public void testPutAndPollTimestampOrdering() throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(2, 0, 0, "m0"),
                                                  message(2, 1, 1, "m1"),
                                                  message(2, 2, 2, "m2"),
                                                  message(2, 3, 8, "m8"),
                                                  message(2, 4, 9, "m9"),
                                                  message(2, 5, 11, "m11"),
                                                  message(0, 0, 3, "m3"),
                                                  message(0, 1, 4, "m4"),
                                                  message(0, 2, 5, "m5"),
                                                  message(0, 3, 10, "m10"),
                                                  message(0, 4, 7, "m7"),
                                                  message(1, 0, 6, "m6"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.poll(0, NANOSECONDS).value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPutAndTakeTimestampOrdering() throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(2, 0, 0, "m0"),
                                                  message(2, 1, 1, "m1"),
                                                  message(2, 2, 2, "m2"),
                                                  message(2, 3, 8, "m8"),
                                                  message(2, 4, 9, "m9"),
                                                  message(2, 5, 11, "m11"),
                                                  message(0, 0, 3, "m3"),
                                                  message(0, 1, 4, "m4"),
                                                  message(0, 2, 5, "m5"),
                                                  message(0, 3, 10, "m10"),
                                                  message(0, 4, 7, "m7"),
                                                  message(1, 0, 6, "m6"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.take().value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPutAndPollProgressiveBuffer() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertNull(buffer.poll(10, NANOSECONDS));
        buffer.put(message(0, 10, 10, "m10"));
        buffer.put(message(0, 11, 11, "m11"));
        buffer.put(message(1, 20, 5, "m5"));
        buffer.put(message(100, 0, 0, "m0"));
        assertEquals("m0", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m5", buffer.poll(0, NANOSECONDS).value().getPayload());
        buffer.put(message(0, 9, 9, "m9"));
        assertEquals("m9", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m10", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m11", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertNull(new SortedKafkaMessageBuffer<>().poll(0, NANOSECONDS));
    }

    public void testPutAndTakeProgressiveBuffer() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        buffer.put(message(0, 10, 10, "m10"));
        buffer.put(message(0, 11, 11, "m11"));
        buffer.put(message(1, 20, 5, "m5"));
        buffer.put(message(100, 0, 0, "m0"));
        assertEquals("m0", buffer.take().value().getPayload());
        assertEquals("m5", buffer.take().value().getPayload());
        buffer.put(message(0, 9, 9, "m9"));
        assertEquals("m9", buffer.take().value().getPayload());
        assertEquals("m10", buffer.take().value().getPayload());
        assertEquals("m11", buffer.take().value().getPayload());
        assertNull(new SortedKafkaMessageBuffer<>().poll(0, NANOSECONDS));
    }

    public void testPutAndPollMessagesPublishedAtSameTimeOnSamePartition() throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(0, 0, 1, "m0"),
                                                  message(0, 1, 1, "m1"),
                                                  message(0, 2, 1, "m2"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.poll(0, NANOSECONDS).value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPutAndTakeMessagesPublishedAtSameTimeOnSamePartition() throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(0, 0, 1, "m0"),
                                                  message(0, 1, 1, "m1"),
                                                  message(0, 2, 1, "m2"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.take().value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPutAndPollMessagesPublishedAtSameTimeAcrossDifferentPartitions() throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(0, 0, 1, "m0"),
                                                  message(1, 0, 1, "m1"),
                                                  message(2, 0, 1, "m2"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.poll(0, NANOSECONDS).value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPutAndTakeMessagesPublishedAtSameTimeAcrossDifferentPartitions()
            throws InterruptedException {
        List<KafkaEventMessage> messages = asList(message(0, 0, 1, "m0"),
                                                  message(1, 0, 1, "m1"),
                                                  message(2, 0, 1, "m2"));
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        for (int i = 0; i < messages.size(); i++) {
            buffer.put(messages.get(i));
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < messages.size(); i++) {
            Object payload = buffer.take().value().getPayload();
            assertEquals("m" + i, payload);
            assertEquals(messages.size() - (i + 1), buffer.size());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPeekAndPollProgressiveBuffer() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        assertNull(buffer.peek());
        //insert two messages - timestamp in ASCENDING order
        buffer.put(message(0, 10, 10, "m10"));
        assertEquals("m10", buffer.peek().value().getPayload());
        buffer.put(message(0, 11, 11, "m11"));
        assertEquals("m10", buffer.peek().value().getPayload());
        //insert two messages - timestamp in DESCENDING order
        buffer.put(message(1, 20, 5, "m5"));
        assertEquals("m5", buffer.peek().value().getPayload());
        buffer.put(message(100, 0, 0, "m0"));
        assertEquals("m0", buffer.peek().value().getPayload());
        //remove m0
        assertEquals("m0", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m5", buffer.peek().value().getPayload());
        //remove m5
        assertEquals("m5", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m10", buffer.peek().value().getPayload());
        //add m9
        buffer.put(message(0, 9, 9, "m9"));
        assertEquals("m9", buffer.peek().value().getPayload());
        //remove m9
        assertEquals("m9", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m10", buffer.peek().value().getPayload());
        //remove m10
        assertEquals("m10", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertEquals("m11", buffer.peek().value().getPayload());
        //finally remove last message(m11)
        assertEquals("m11", buffer.poll(0, NANOSECONDS).value().getPayload());
        assertNull(buffer.peek());
        assertTrue(buffer.isEmpty());
    }

    public void testPeekMessagesPublishedAtTheSameTimeAcrossDifferentPartitions() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        List<KafkaEventMessage> messages = asList(message(0, 0, 1, "m0"),
                                                  message(2, 1, 1, "m1"),
                                                  message(1, 0, 1, "m2")
        );
        assertNull(new SortedKafkaMessageBuffer<>().peek());
        for (KafkaEventMessage message : messages) {
            buffer.put(message);
        }
        assertEquals("m0", buffer.peek().value().getPayload());
        buffer.poll(0, NANOSECONDS);
        assertEquals("m2", buffer.peek().value().getPayload());
        buffer.poll(0, NANOSECONDS);
        assertEquals("m1", buffer.peek().value().getPayload());
        buffer.poll(0, NANOSECONDS);
        assertTrue(buffer.isEmpty());
    }

    public void testPollOffsetOrdering() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        List<KafkaEventMessage> messages = asList(message(1, 1, 1, "m-p1"),
                                                  message(0, 1, 2, "m-p0-1"),
                                                  message(0, 0, 2, "m-p0-0"),
                                                  message(2, 2, 0, "m-p2"));
        for (KafkaEventMessage message : messages) {
            buffer.put(message);
        }
        // m-p2, published at T0
        // m-p1, published at T1
        // m-p0-0, published at T2.
        // m-p0-1, also published at T2 but has offset(1) greater than m-p0-0 offset(0).
        List<KafkaEventMessage> ordered = asList(message(2, 2, 0, "m-p2"),
                                                 message(1, 1, 1, "m-p1"),
                                                 message(0, 0, 2, "m-p0-0"),
                                                 message(0, 1, 2, "m-p0-1"));

        for (int i = 0; i < messages.size(); i++) {
            assertEquals(ordered.get(i).value().getPayload(), buffer.poll(0, MILLISECONDS).value().getPayload());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testTakeOffsetOrdering() throws InterruptedException {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        List<KafkaEventMessage> messages = asList(message(1, 1, 1, "m-p1"),
                                                  message(0, 1, 2, "m-p0-1"),
                                                  message(0, 0, 2, "m-p0-0"),
                                                  message(2, 2, 0, "m-p2"));
        for (KafkaEventMessage message : messages) {
            buffer.put(message);
        }
        // m-p2, published at T0
        // m-p1, published at T1
        // m-p0-0, published at T2.
        // m-p0-1, also published at T2 but has offset(1) greater than m-p0-0 offset(0).
        List<KafkaEventMessage> ordered = asList(message(2, 2, 0, "m-p2"),
                                                 message(1, 1, 1, "m-p1"),
                                                 message(0, 0, 2, "m-p0-0"),
                                                 message(0, 1, 2, "m-p0-1"));

        for (int i = 0; i < messages.size(); i++) {
            assertEquals(ordered.get(i).value().getPayload(), buffer.take().value().getPayload());
        }
        assertTrue(buffer.isEmpty());
    }

    public void testPollOnAnInterruptedStream() {
        try {
            SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> buffer.poll(0, NANOSECONDS));
        } finally {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
    }

    public void testPeekOnAnInterruptedStream() {
        try {
            SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
            Thread.currentThread().interrupt();
            assertNull(buffer.peek());
        } finally {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
    }

    public void testPutOnAnInterruptedStream() {
        try {
            SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> buffer.put(message(0, 0, 1, "foo")));
        } finally {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
    }

    public void testTakeOnAnInterruptedStream() {
        try {
            SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, buffer::take);
        } finally {
            //noinspection ResultOfMethodCallIgnored
            Thread.interrupted();
        }
    }

    public void testConcurrentPeekAndPoll() throws Throwable {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        AtomicInteger partition = new AtomicInteger(0);
        concurrent(4, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    buffer.put(message(partition.getAndIncrement(), i, i + 1, "m"));
                } catch (InterruptedException e) {
                    fail("This shouldn't happen");
                }
                assertTrue(buffer.size() > 0);
                assertNotNull(buffer.peek());
                try {
                    assertNotNull(buffer.poll(0, TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    fail("This shouldn't happen");
                }
            }
        });
        assertTrue(buffer.isEmpty());
    }

    public void testConcurrentPeekAndTake() throws Throwable {
        SortedKafkaMessageBuffer<KafkaEventMessage> buffer = new SortedKafkaMessageBuffer<>();
        AtomicInteger partition = new AtomicInteger(0);
        concurrent(4, () -> {
            for (int i = 0; i < 100; i++) {
                try {
                    buffer.put(message(partition.getAndIncrement(), i, i + 1, "m"));
                } catch (InterruptedException e) {
                    fail("This shouldn't happen");
                }
                assertTrue(buffer.size() > 0);
                assertNotNull(buffer.peek());
                try {
                    assertNotNull(buffer.take());
                } catch (InterruptedException e) {
                    fail("This shouldn't happen");
                }
            }
        });
        assertTrue(buffer.isEmpty());
    }

    public void testExceptionThrowOnTakeWhenSet() {
        final SortedKafkaMessageBuffer<KafkaEventMessage> buff = new SortedKafkaMessageBuffer<>(SIZE);
        buff.setException(new FetchEventException("something"));
        assertThrows(FetchEventException.class, buff::take);
    }

    public void testExceptionThrowOnPeekWhenSet() {
        final SortedKafkaMessageBuffer<KafkaEventMessage> buff = new SortedKafkaMessageBuffer<>(SIZE);
        buff.setException(new FetchEventException("something"));
        assertThrows(FetchEventException.class, buff::peek);
    }

    public void testExceptionThrowOnPollWhenSet() {
        final SortedKafkaMessageBuffer<KafkaEventMessage> buff = new SortedKafkaMessageBuffer<>(SIZE);
        buff.setException(new FetchEventException("something"));
        assertThrows(FetchEventException.class, () -> buff.poll(10L, TimeUnit.SECONDS));
    }

    public void testOnceExceptionSetTakeStillFirstEmptiesBuffer() throws InterruptedException {
        final SortedKafkaMessageBuffer<KafkaEventMessage> buff = new SortedKafkaMessageBuffer<>(SIZE);
        buff.put(message(0, 1, 0, "m"));
        buff.setException(new FetchEventException("something"));
        buff.take();
        assertThrows(FetchEventException.class, buff::take);
    }
}
