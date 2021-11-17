/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.infiniteautomation.mango.pointvalue.generator.BatchPointValueSupplier;
import com.infiniteautomation.mango.pointvalue.generator.BrownianPointValueGenerator;

@Fork(value = 1, warmups = 0)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 0, time = 300)
@Measurement(iterations = 1, time = 300)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Insert extends TsdbBenchmark {

    @Test
    public void runBenchmark() throws RunnerException {
        runBenchmark(new OptionsBuilder().build());
    }

    @State(Scope.Thread)
    public static class InsertParams {

        long startTimestamp;
        long invocationCount;
        List<BatchPointValueSupplier> suppliers;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.startTimestamp = ZonedDateTime.parse(mango.startDate).toInstant().toEpochMilli();
            var points = mango.createDataPoints(mango.points / mango.threads, Collections.emptyMap());
            var generator = new BrownianPointValueGenerator(startTimestamp, mango.period);
            this.suppliers = points.stream().map(generator::createSupplier).collect(Collectors.toList());
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() {
            this.invocationCount++;
        }
    }

    @Benchmark
    public void insert(TsdbMockMango mango, InsertParams insertParams) {
        for (var supplier : insertParams.suppliers) {
            mango.pvDao.savePointValues(supplier.stream().limit(mango.batchSize), mango.batchSize);
        }
    }

    @Benchmark
    public void withBackdates(TsdbMockMango mango, InsertParams insertParams) {
        // every x invocations insert some backdated values, this leaves holes and back-fills them without overwriting
        int backdateFrequency = 10;
        boolean backdate = insertParams.invocationCount % backdateFrequency == 0;

        for (var supplier : insertParams.suppliers) {
            long ts = supplier.getTimestamp();
            if (backdate) {
                // rewind timestamp
                supplier.setTimestamp(ts - backdateFrequency * mango.period * mango.batchSize);
            }
            mango.pvDao.savePointValues(supplier.stream().limit(mango.batchSize), mango.batchSize);
            if (backdate) {
                // restore timestamp position
                supplier.setTimestamp(ts + mango.period * mango.batchSize);
            }
        }
    }
}
