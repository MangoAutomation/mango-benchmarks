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
    public static class InsertState {

        final Random random = new Random();
        List<DataPointVO> points;
        int index = 0;

        @Setup
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.points = mango.createDataPoints(mango.points / mango.threads, Collections.emptyMap());
        }

        public PointValueTime newValue(long timestamp) {
            return new PointValueTime(random.nextDouble(), timestamp);
        }
    }

    @Benchmark
    public void insert(TsdbMockMango mango, InsertState insertState, Blackhole blackhole) {
        long timestamp = insertState.index * 5000L;
        for (DataPointVO point : insertState.points) {
            PointValueTime v = mango.pvDao.savePointValueSync(point, insertState.newValue(timestamp), null);
            blackhole.consume(v);
        }
        insertState.index++;
    }

    @Benchmark
    public void backdates(TsdbMockMango mango, InsertState insertState, Blackhole blackhole) {
        long timestamp = insertState.index * 5000L;
        // every 1000 iterations insert a backdated value
        if (insertState.index % 1000 == 0) {
            timestamp -= 1000 * 5000L;
        }
        for (DataPointVO point : insertState.points) {
            PointValueTime v = mango.pvDao.savePointValueSync(point, insertState.newValue(timestamp), null);
            blackhole.consume(v);
        }
        insertState.index++;
    }
}