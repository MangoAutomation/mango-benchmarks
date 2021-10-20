/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.serotonin.m2m2.rt.dataImage.PointValueTime;
import com.serotonin.m2m2.vo.DataPointVO;

@Fork(value = 1, warmups = 0)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 60)
@Measurement(iterations = 5, time = 60)
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

        final int valuesPerPoint = 10_000;
        final int valuesPerRead = 10;
        final int interval = 1000;
        final long startTimestamp = 0;
        final long endTimestamp = startTimestamp + valuesPerPoint * interval;

        long readStart = 0;
        long readEnd = 0;
        final Random random = new Random();
        List<DataPointVO> points;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            int pointsPerThread = mango.points / mango.threads;

            this.points = mango.createDataPoints(pointsPerThread, Collections.emptyMap());
            for (DataPointVO point : points) {
                for (int i = 0; i < valuesPerPoint; i++) {
                    mango.pvDao.savePointValueAsync(point, new PointValueTime(random.nextDouble(), readStart + i * interval), null);
                }
            }

            System.out.printf("Saved %d values (for %d points)%n", valuesPerPoint * pointsPerThread, pointsPerThread);
            mango.pvDao.flushPointValues();
            System.out.printf("Finished flushing %d values%n", valuesPerPoint * pointsPerThread);
        }

        @Setup(Level.Invocation)
        public void nextRead() {
            this.readStart = readEnd;
            this.readEnd = readStart + valuesPerRead * interval;

            // loop back to start of data
            if (readEnd > endTimestamp) {
                readEnd = 0;
                nextRead();
            }
        }
    }

    @Benchmark
    public void wideBookendQuery(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.wideBookendQueryCombined(readState.points, readState.readStart, readState.readEnd, null, blackhole::consume);
    }
}
