/*
 * Copyright (c) 2010-2023. Axon Framework
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