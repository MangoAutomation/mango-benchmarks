/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
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

import com.infiniteautomation.mango.pointvalue.generator.BrownianPointValueGenerator;
import com.infiniteautomation.mango.pointvalue.generator.PointValueGenerator;
import com.serotonin.m2m2.db.dao.PointValueDao.TimeOrder;
import com.serotonin.m2m2.vo.DataPointVO;

@Fork(value = 1, warmups = 0)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 0, time = 300)
@Measurement(iterations = 1, time = 300)
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

        long startTimestamp;
        long endTimestamp;

        long readStart;
        long readEnd;

        List<DataPointVO> points;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            this.startTimestamp = ZonedDateTime.parse(mango.startDate).toInstant().toEpochMilli();
            this.endTimestamp = startTimestamp + valuesInsertedPerPoint * mango.period;
            int pointsPerThread = mango.points / mango.threads;

            PointValueGenerator generator = new BrownianPointValueGenerator(startTimestamp, mango.period);
            this.points = mango.createDataPoints(pointsPerThread, Collections.emptyMap());
            for (DataPointVO point : points) {
                var stream = generator.apply(point).limit(valuesInsertedPerPoint);
                mango.pvDao.savePointValues(stream);
            }
            System.out.printf("Saved %d values (for %d points)%n", valuesInsertedPerPoint * pointsPerThread, pointsPerThread);
        }

        @Setup(Level.Iteration)
        public void setupIteration() {
            this.readStart = startTimestamp;
            this.readEnd = startTimestamp;
        }

        @Setup(Level.Invocation)
        public void nextRead(TsdbMockMango mango) {
            this.readStart = readEnd;
            this.readEnd = readStart + mango.batchSize * mango.period;

            // loop back to start of data
            if (readEnd > endTimestamp) {
                this.readStart = startTimestamp;
                this.readEnd = readStart + mango.batchSize * mango.period;
            }
        }
    }

    @Benchmark
    public void forwardReadCombined(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.getPointValuesCombined(readState.points, readState.readStart, readState.readEnd, null, TimeOrder.ASCENDING, blackhole::consume);
    }

    @Benchmark
    public void forwardReadPerPoint(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.getPointValuesPerPoint(readState.points, readState.readStart, readState.readEnd, null, TimeOrder.ASCENDING, blackhole::consume);
    }

    @Benchmark
    public void reverseReadCombined(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.getPointValuesCombined(readState.points, readState.readStart, readState.readEnd, null, TimeOrder.DESCENDING, blackhole::consume);
    }

    @Benchmark
    public void reverseReadPerPoint(TsdbMockMango mango, ReadState readState, Blackhole blackhole) {
        mango.pvDao.getPointValuesPerPoint(readState.points, readState.readStart, readState.readEnd, null, TimeOrder.DESCENDING, blackhole::consume);
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
