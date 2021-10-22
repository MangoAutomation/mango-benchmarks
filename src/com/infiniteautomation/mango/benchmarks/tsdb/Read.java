/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
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
@Warmup(iterations = 1, time = 60)
@Measurement(iterations = 3, time = 10)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Read extends TsdbBenchmark {

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        CommandLineOptions cmdOptions = new CommandLineOptions(args);
        new Read().runBenchmark(cmdOptions);
    }

    @Test
    public void runBenchmark() throws RunnerException {
        runBenchmark(new OptionsBuilder().build());
    }

    @State(Scope.Thread)
    public static class ReadState {

        @Param("10000")
        int valuesInsertedPerPoint;

        final long interval = 5000;
        final long startTimestamp = 0;

        int batchSize;
        long endTimestamp;
        long readStart;
        long readEnd;

        final Random random = new Random();
        List<DataPointVO> points;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.endTimestamp = startTimestamp + valuesInsertedPerPoint * interval;
            this.batchSize = mango.batchSize;
            int pointsPerThread = mango.points / mango.threads;

            this.points = mango.createDataPoints(pointsPerThread, Collections.emptyMap());
            for (DataPointVO point : points) {
                List<BatchPointValue> values = new ArrayList<>(valuesInsertedPerPoint);
                for (int i = 0; i < valuesInsertedPerPoint; i++) {
                    values.add(new BatchPointValueImpl(point, new PointValueTime(random.nextDouble(), readStart + i * interval)));
                }
                mango.pvDao.savePointValues(values.stream());
            }
            System.out.printf("Saved %d values (for %d points)%n", valuesInsertedPerPoint * pointsPerThread, pointsPerThread);
        }

        @Setup(Level.Invocation)
        public void nextRead() {
            this.readStart = readEnd;
            this.readEnd = readStart + batchSize * interval;

            // loop back to start of data
            if (readEnd > endTimestamp) {
                this.readStart = 0;
                this.readEnd = batchSize * interval;
            }
        }
    }

    @Benchmark
    public void wideBookendQueryCombined(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.wideBookendQueryCombined(readState.points, readState.readStart, readState.readEnd, null, blackhole::consume);
    }

    @Benchmark
    public void wideBookendQueryPerPoint(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.wideBookendQueryPerPoint(readState.points, readState.readStart, readState.readEnd, null, blackhole::consume);
    }
}
