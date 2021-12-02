/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

    @State(Scope.Thread)
    public static class ReadState {

        @Param("10000")
        int valuesInsertedPerPoint;

        long startTimestamp;
        long endTimestamp;

        long readStart;
        long readEnd;

        /**
         * all points for this thread
         */
        List<DataPointVO> allPoints;
        int index = 0;
        /**
         *  points read within a single invocation
         */
        List<DataPointVO> points;

        @Setup(Level.Trial)
        public void setup(TsdbMockMango mango) throws ExecutionException, InterruptedException {
            long start = System.nanoTime();
            this.startTimestamp = ZonedDateTime.parse(mango.startDate).toInstant().toEpochMilli();
            this.endTimestamp = startTimestamp + valuesInsertedPerPoint * mango.period;
            int pointsPerThread = mango.totalPoints / mango.threads;

            PointValueGenerator generator = new BrownianPointValueGenerator(startTimestamp, mango.period);
            this.allPoints = mango.createDataPoints(pointsPerThread, Collections.emptyMap());
            for (DataPointVO point : allPoints) {
                var stream = generator.apply(point).limit(valuesInsertedPerPoint);
                mango.pvDao.savePointValues(stream);
            }
            long duration = System.nanoTime() - start;
            System.out.printf("Thread '%s' inserted %d values (for %d points, %d values per point) in %.2f seconds.%n",
                    Thread.currentThread().getName(), valuesInsertedPerPoint * pointsPerThread, pointsPerThread, valuesInsertedPerPoint, TimeUnit.NANOSECONDS.toMillis(duration) / 1000.0);
        }

        @Setup(Level.Iteration)
        public void setupIteration(TsdbMockMango mango) {
            this.readStart = startTimestamp;
            this.readEnd = readStart + mango.batchSize * mango.period;
        }

        @Setup(Level.Invocation)
        public void nextRead(TsdbMockMango mango) {
            int endIndex = index + mango.points;
            if (endIndex > allPoints.size()) {
                this.index = 0;
                endIndex = mango.points;

                // move to next time range
                this.readStart = readEnd;
                this.readEnd = readStart + mango.batchSize * mango.period;

                // loop back to start of data
                if (readEnd > endTimestamp) {
                    this.readStart = startTimestamp;
                    this.readEnd = readStart + mango.batchSize * mango.period;
                }
            }
            this.points = allPoints.subList(index, endIndex);
            this.index = endIndex;
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
