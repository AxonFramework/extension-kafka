/*
 * Copyright (c) 2010-2021. Axon Framework
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

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

/*
 * Source:
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/test/tck/JSR166TestCase.java?revision=1.90
 * (We have made some trivial local modifications (commented out
 * uncompilable code).)
 */

package org.axonframework.extensions.kafka.eventhandling.consumer.streamable;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for JSR166 Junit TCK tests. Defines some constants, utility methods and classes, as well as a simple
 * framework for helping to make sure that assertions failing in generated threads cause the associated test that
 * generated them to itself fail (which JUnit does not otherwise arrange). The rules for creating such tests are:
 *
 * <ol>
 *   <li>All assertions in code running in generated threads must use the forms {@code #threadFail},
 *       {@code #threadAssertTrue}, {@code #threadAssertEquals}, or {@code #threadAssertNull}, (not
 *       {@code fail}, {@code assertTrue}, etc.) It is OK (but not particularly recommended) for
 *       other code to use these forms too. Only the most typically used JUnit assertion methods are
 *       defined this way, but enough to live with.
 *   <li>If you override {@link #setUp} or {@link #tearDown}, make sure to invoke {@code
 *       super.setUp} and {@code super.tearDown} within them. These methods are used to clear and
 *       check for thread assertion failures.
 *   <li>All delays and timeouts must use one of the constants {@code SHORT_DELAY_MS}, {@code
 *       SMALL_DELAY_MS}, {@code MEDIUM_DELAY_MS}, {@code LONG_DELAY_MS}. The idea here is that a
 *       SHORT is always discriminable from zero time, and always allows enough time for the small
 *       amounts of computation (creating a thread, calling a few methods, etc) needed to reach a
 *       timeout point. Similarly, a SMALL is always discriminable as larger than SHORT and smaller
 *       than MEDIUM. And so on. These constants are set to conservative values, but even so, if
 *       there is ever any doubt, they can all be increased in one spot to rerun tests on slower
 *       platforms.
 *   <li>All threads generated must be joined inside each test case method (or {@code fail} to do
 *       so) before returning from the method. The {@code joinPool} method can be used to do this
 *       when using Executors.
 * </ol>
 *
 * <p><b>Other notes</b>
 *
 * <ul>
 *   <li>Usually, there is one testcase method per JSR166 method covering "normal" operation, and
 *       then as many exception-testing methods as there are exceptions the method can throw.
 *       Sometimes there are multiple tests per JSR166 method when the different "normal" behaviors
 *       differ significantly. And sometimes testcases cover multiple methods when they cannot be
 *       tested in isolation.
 *   <li>The documentation style for testcases is to provide as javadoc a simple sentence or two
 *       describing the property that the testcase method purports to test. The javadocs do not say
 *       anything about how the property is tested. To find out, read the code.
 *   <li>These tests are "conformance tests", and do not attempt to test throughput, latency,
 *       scalability or other performance factors (see the separate "jtreg" tests for a set intended
 *       to check these for the most central aspects of functionality.) So, most tests use the
 *       smallest sensible numbers of threads, collection sizes, etc needed to check basic
 *       conformance.
 *   <li>The test classes currently do not declare inclusion in any particular package to simplify
 *       things for people integrating them in TCK test suites.
 *   <li>As a convenience, the {@code main} of this class (JSR166TestCase) runs all JSR166 unit
 *       tests.
 * </ul>
 */
@Disabled
abstract class JSR166TestCase extends TestCase {

    /**
     * If true, report on stdout all "slow" tests, that is, ones that take more than profileThreshold milliseconds to
     * execute.
     */
    private static final boolean profileTests = Boolean.getBoolean("jsr166.profileTests");

    /**
     * The number of milliseconds that tests are permitted for execution without being reported, when profileTests is
     * set.
     */
    private static final long profileThreshold = Long.getLong("jsr166.profileThreshold", 100);

    protected void runTest() throws Throwable {
        if (profileTests) {
            runTestProfiled();
        } else {
            super.runTest();
        }
    }

    protected void runTestProfiled() throws Throwable {
        long t0 = System.nanoTime();
        try {
            super.runTest();
        } finally {
            long elapsedMillis = (System.nanoTime() - t0) / (1000L * 1000L);
            if (elapsedMillis >= profileThreshold) {
                System.out.printf("%n%s: %d%n", this, elapsedMillis);
            }
        }
    }

    public static long SHORT_DELAY_MS;
    public static long SMALL_DELAY_MS;
    public static long MEDIUM_DELAY_MS;
    public static long LONG_DELAY_MS;

    /**
     * Returns the shortest timed delay. This could be reimplemented to use for example a Property.
     */
    protected long getShortDelay() {
        return 50;
    }

    /**
     * Sets delays as multiples of SHORT_DELAY.
     */
    protected void setDelays() {
        SHORT_DELAY_MS = getShortDelay();
        SMALL_DELAY_MS = SHORT_DELAY_MS * 5;
        MEDIUM_DELAY_MS = SHORT_DELAY_MS * 10;
        LONG_DELAY_MS = SHORT_DELAY_MS * 200;
    }

    /**
     * The first exception encountered if any threadAssertXXX method fails.
     */
    private final AtomicReference<Throwable> threadFailure = new AtomicReference<>(null);

    /**
     * Records an exception so that it can be rethrown later in the test harness thread, triggering a test case failure.
     * Only the first failure is recorded; subsequent calls to this method from within the same test have no effect.
     */
    public void threadRecordFailure(Throwable t) {
        threadFailure.compareAndSet(null, t);
    }

    public void setUp() {
        setDelays();
    }

    /**
     * Extra checks that get done for all test cases.
     *
     * <p>Triggers test case failure if any thread assertions have failed, by rethrowing, in the test
     * harness thread, any exception recorded earlier by threadRecordFailure.
     *
     * <p>Triggers test case failure if interrupt status is set in the main thread.
     */
    public void tearDown() throws Exception {
        Throwable t = threadFailure.getAndSet(null);
        if (t != null) {
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Exception) {
                throw (Exception) t;
            } else {
                AssertionFailedError afe = new AssertionFailedError(t.toString());
                afe.initCause(t);
                throw afe;
            }
        }

        if (Thread.interrupted()) {
            throw new AssertionFailedError("interrupt status set in main thread");
        }
    }

    /**
     * Records the given exception using {@link #threadRecordFailure}, then rethrows the exception, wrapping it in an
     * AssertionFailedError if necessary.
     */
    public void threadUnexpectedException(Throwable t) {
        threadRecordFailure(t);
        t.printStackTrace();
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            AssertionFailedError afe = new AssertionFailedError("unexpected exception: " + t);
            afe.initCause(t);
            throw afe;
        }
    }

    /**
     * Delays, via Thread.sleep(), for the given millisecond delay, but if the sleep is shorter than specified, may
     * re-sleep or yield until time elapses.
     */
    @SuppressWarnings({"SameParameterValue", "squid:S2925"})
    static void delay(long millis) throws InterruptedException {
        long startTime = System.nanoTime();
        long ns = millis * 1000 * 1000;
        for (; ; ) {
            if (millis > 0L) {
                //noinspection BusyWait
                Thread.sleep(millis);
            } else // too short to sleep
            {
                Thread.yield();
            }
            long d = ns - (System.nanoTime() - startTime);
            if (d > 0L) {
                millis = d / (1000 * 1000);
            } else {
                break;
            }
        }
    }


    /**
     * The number of elements to place in collections, arrays, etc.
     */
    public static final int SIZE = 20;

    // Some convenient Integer constants

    public static final Integer m3 = -3;
    public static final Integer m4 = -4;

    /**
     * Returns a new started daemon Thread running the given runnable.
     */
    Thread newStartedThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Waits for the specified time (in milliseconds) for the thread to terminate (using {@link Thread#join(long)}),
     * else interrupts the thread (in the hope that it may terminate later) and fails.
     */
    void awaitTermination(Thread t, long timeoutMillis) {
        try {
            t.join(timeoutMillis);
        } catch (InterruptedException ie) {
            threadUnexpectedException(ie);
        } finally {
            if (t.getState() != Thread.State.TERMINATED) {
                t.interrupt();
                fail("Test timed out");
            }
        }
    }

    /**
     * Waits for LONG_DELAY_MS milliseconds for the thread to terminate (using {@link Thread#join(long)}), else
     * interrupts the thread (in the hope that it may terminate later) and fails.
     */
    void awaitTermination(Thread t) {
        awaitTermination(t, LONG_DELAY_MS);
    }

    public abstract class CheckedRunnable implements Runnable {

        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
            } catch (Throwable t) {
                threadUnexpectedException(t);
            }
        }
    }


    public void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(LONG_DELAY_MS, MILLISECONDS));
        } catch (Throwable t) {
            threadUnexpectedException(t);
        }
    }
}