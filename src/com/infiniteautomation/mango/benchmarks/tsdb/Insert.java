/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.infiniteautomation.mango.pointvalue.generator.BatchPointValueSupplier;
import com.infiniteautomation.mango.pointvalue.generator.BrownianPointValueGenerator;

@Fork(value = 1, warmups = 0)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 0, time = 300)
@Measurement(iterations = 1, time = 300)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Insert extends TsdbBenchmark {

    @State(Scope.Thread)
    public static class InsertParams {

        long startTimestamp;
        long invocationCount;
        List<BatchPointValueSupplier> suppliers;
        Iterator<BatchPointValueSupplier> it;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            long start = System.nanoTime();
            this.startTimestamp = ZonedDateTime.parse(mango.startDate).toInstant().toEpochMilli();
            var points = mango.createDataPoints(mango.totalPoints / mango.threads, Collections.emptyMap());
            var generator = new BrownianPointValueGenerator(startTimestamp, mango.period);
            this.suppliers = points.stream().map(generator::createSupplier).collect(Collectors.toList());
            long duration = System.nanoTime() - start;
            System.out.printf("Thread '%s' created %d points in %.2f seconds.%n",
                    Thread.currentThread().getName(), mango.totalPoints / mango.threads, TimeUnit.NANOSECONDS.toMillis(duration) / 1000.0);
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() {
            this.invocationCount++;
        }

        /**
         * @return the next supplier from the list, looping back to start if end of list reached
         */
        public BatchPointValueSupplier next() {
            if (it == null || !it.hasNext()) {
                it = suppliers.iterator();
            }
            return it.next();
        }
    }

    @Benchmark
    public void insert(TsdbMockMango mango, InsertParams insertParams) {
        for (int i = 0; i < mango.points; i++) {
            var supplier = insertParams.next();
            mango.pvDao.savePointValues(supplier.stream().limit(mango.batchSize), mango.batchSize);
        }
    }

    @Benchmark
    public void withBackdates(TsdbMockMango mango, InsertParams insertParams) {
        // every x invocations insert some backdated values, this leaves holes and back-fills them without overwriting
        int backdateFrequency = 10;
        long batchPeriod = mango.period * mango.batchSize;
        long backdatePeriod = backdateFrequency * batchPeriod;
        long startOffset = insertParams.startTimestamp % backdatePeriod;

        for (int i = 0; i < mango.points; i++) {
            var supplier = insertParams.next();
            long ts = supplier.getTimestamp();
            if (ts % backdatePeriod == startOffset) {
                // rewind timestamp
                supplier.setTimestamp(ts - backdatePeriod);
            }

            mango.pvDao.savePointValues(supplier.stream().limit(mango.batchSize), mango.batchSize);

            // restore timestamp position
            supplier.setTimestamp(ts + batchPeriod);
        }
    }
}
