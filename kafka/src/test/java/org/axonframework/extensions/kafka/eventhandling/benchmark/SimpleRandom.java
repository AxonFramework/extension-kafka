package org.axonframework.extensions.kafka.eventhandling.benchmark;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Pavel Tcholakov.
 * @see <a href="https://github.com/JCTools/JCTools">JCTools</a>
 */
final class SimpleRandom {

    private final static long multiplier = 0x5DEECE66DL;
    private final static long addend = 0xBL;
    private final static long mask = (1L << 48) - 1;
    private static final AtomicLong seq = new AtomicLong(-715159705);
    private long seed;

    SimpleRandom() {
        seed = System.nanoTime() + seq.getAndAdd(129);
    }

    public int next() {
        long nextSeed = (seed * multiplier + addend) & mask;
        seed = nextSeed;
        return ((int) (nextSeed >>> 17)) & 0x7FFFFFFF;
    }
}