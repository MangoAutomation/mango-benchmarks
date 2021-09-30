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
        long readStart = 0;
        long readEnd = readStart + valuesPerRead * interval;
        final Random random = new Random();
        List<DataPointVO> points;

        @Setup
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.points = mango.createDataPoints(mango.points / mango.threads, Collections.emptyMap());
            for (DataPointVO point : points) {
                for (int i = 0; i < valuesPerPoint; i++) {
                    mango.pvDao.savePointValueSync(point, new PointValueTime(random.nextDouble(), readStart + i * interval), null);
                }
            }
            System.out.printf("Finished inserting %d values for %d points%n", valuesPerPoint, mango.points / mango.threads);
        }

        private void nextRead() {
            this.readStart = readEnd;
            this.readEnd = readStart + valuesPerRead * interval;
        }
    }

    @Benchmark
    public void wideBookendQuery(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.wideBookendQuery(readState.points, readState.readStart, readState.readEnd, false, null, (value) -> {
            blackhole.consume(value);
        });
        readState.nextRead();
    }
}
