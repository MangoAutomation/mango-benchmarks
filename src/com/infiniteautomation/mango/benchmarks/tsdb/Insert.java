/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.serotonin.m2m2.db.dao.BatchPointValue;
import com.serotonin.m2m2.db.dao.BatchPointValueImpl;
import com.serotonin.m2m2.rt.dataImage.PointValueTime;
import com.serotonin.m2m2.vo.DataPointVO;

@Fork(value = 1, warmups = 0)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 60)
@Measurement(iterations = 5, time = 60)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Insert extends TsdbBenchmark {

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        CommandLineOptions cmdOptions = new CommandLineOptions(args);
        new Insert().runBenchmark(cmdOptions);
    }

    @Test
    public void runBenchmark() throws RunnerException {
        runBenchmark(new OptionsBuilder().build());
    }

    @State(Scope.Thread)
    public static class InsertParams {

        final Random random = new Random();
        List<DataPointVO> points;
        long index = 0;
        final long timeIncrement = 5000L;
        int batchSize;

        @Setup
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.points = mango.createDataPoints(mango.points / mango.threads, Collections.emptyMap());
            this.batchSize = mango.batchSize;
        }

        public PointValueTime newValue(long timestamp) {
            return new PointValueTime(random.nextDouble(), timestamp);
        }

        public Stream<BatchPointValue> generateData(DataPointVO point, long startTime) {
            return Stream.generate(new Supplier<BatchPointValue>() {
                int count;

                @Override
                public BatchPointValue get() {
                    return new BatchPointValueImpl(point, newValue(startTime + count++ * timeIncrement));
                }
            }).limit(batchSize);
        }
    }

    @Benchmark
    public void insert(TsdbMockMango mango, InsertParams insertParams) {
        long startTime = insertParams.index * insertParams.timeIncrement * insertParams.batchSize;
        for (DataPointVO point : insertParams.points) {
            mango.pvDao.savePointValues(insertParams.generateData(point, startTime));
        }
        insertParams.index++;
    }

    @Benchmark
    public void withBackdates(TsdbMockMango mango, InsertParams insertParams) {
        long index = insertParams.index;
        // every 100 iterations insert some backdated values, this leaves holes and back-fills them without overwriting
        if (index % 100 == 0) {
            index -= 100;
        }
        long startTime = index * insertParams.timeIncrement * insertParams.batchSize;
        for (DataPointVO point : insertParams.points) {
            mango.pvDao.savePointValues(insertParams.generateData(point, startTime));
        }
        insertParams.index++;
    }
}
