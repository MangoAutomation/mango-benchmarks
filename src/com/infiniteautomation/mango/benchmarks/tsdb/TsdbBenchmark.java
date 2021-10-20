/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.format.OutputFormat;
import org.openjdk.jmh.runner.format.OutputFormatFactory;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.infiniteautomation.mango.benchmarks.MockMango;
import com.serotonin.m2m2.Common;
import com.serotonin.m2m2.db.dao.PointValueDao;

public abstract class TsdbBenchmark {
    public static final String THREADS_PARAM = "threads";
    public static final String POINTS_PARAM = "points";
    public static final Collection<String> DEFAULT_THREADS = Collections.singletonList("1C");
    public static final Collection<String> DEFAULT_POINTS = Collections.singletonList("1000");
    public final static int NUM_CPU_CORES = Runtime.getRuntime().availableProcessors();

    /**
     * We cannot parameterize the Threads and OperationsPerInvocation annotations, so we must do it ourselves
     * programmatically, combine the results, then output them to stdout
     */
    public void runBenchmark(Options options) throws RunnerException {
        List<RunResult> results = new ArrayList<>();

        int[] threadsParams = options.getParameter(THREADS_PARAM)
                .orElse(DEFAULT_THREADS)
                .stream()
                .mapToInt(TsdbBenchmark::parseCpuMultiplier).toArray();

        int[] pointsParams = options.getParameter(POINTS_PARAM)
                .orElse(DEFAULT_POINTS)
                .stream()
                .mapToInt(TsdbBenchmark::parseCpuMultiplier).toArray();

        for (int threads : threadsParams) {
            for (int points : pointsParams) {
                var builder = new OptionsBuilder()
                        .parent(options)
                        .threads(threads)
                        .operationsPerInvocation(points / threads)
                        .param(THREADS_PARAM, Integer.toString(threads))
                        .param(POINTS_PARAM, Integer.toString(points));

                if (options.getIncludes().isEmpty()) {
                    builder.include(getClass().getName());
                }

                var opts = builder.build();
                results.addAll(new Runner(opts).run());
            }
        }

        // sort the results for more legible output
        results.sort(RunResult.DEFAULT_SORT_COMPARATOR);

        OutputFormat outputFormat = OutputFormatFactory.createFormatInstance(System.out, VerboseMode.NORMAL);
        outputFormat.endRun(results);
    }

    public static int parseCpuMultiplier(String param) {
        return parseMultiplier(param, "C", NUM_CPU_CORES);
    }

    public static int parseMultiplier(String param, String suffix, int multiplicand) {
        if (param.endsWith(suffix)) {
            float multiplier = Float.parseFloat(param.substring(0, param.length() - suffix.length()));
            return (int) multiplier * multiplicand;
        }
        return Integer.parseInt(param);
    }

    @State(Scope.Benchmark)
    public static class TsdbMockMango extends MockMango {
        //@Param({"h2:memory", "h2", "mysql})
        @Param({"h2", "mysql"})
        String databaseType;

        //@Param({"ias-tsdb", "sql", "tsl"})
        @Param({"sql"})
        String implementation;

        /**
         * This default is overridden via {@link TsdbBenchmark#DEFAULT_THREADS}
         */
        @Param({"1"})
        int threads;

        /**
         * This default is overridden via {@link TsdbBenchmark#DEFAULT_POINTS}
         */
        @Param({"1"})
        int points;

        @Param({"2X"})
        String maxOpenFiles;

//        @Param({"INPUT_STREAM", "FILE_CHANNEL", "RANDOM_ACCESS_FILE", "MAPPED_BYTE_BUFFER"})
        @Param({"MAPPED_BYTE_BUFFER"})
        String shardStreamType;

        PointValueDao pvDao;
        JdbcDatabaseContainer<?> jdbcContainer;

        @Override
        protected void preInitialize() {
            int maxOpenFiles = parseMultiplier(this.maxOpenFiles, "X", points);
            properties.setProperty("db.nosql.maxOpenFiles", Integer.toString(maxOpenFiles));
            properties.setProperty("db.nosql.shardStreamType", shardStreamType);

            properties.setProperty("db.type", databaseType);
            if (jdbcContainer != null) {
                properties.setProperty("db.url", jdbcContainer.getJdbcUrl());
                properties.setProperty("db.username", jdbcContainer.getUsername());
                properties.setProperty("db.password", jdbcContainer.getPassword());
            } else if ("h2:memory".equals(databaseType)) {
                properties.setProperty("db.type", "h2");
                // default url in test environment is in-memory url, however lets not set the LOCK_MODE parameter
                properties.setProperty("db.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            } else if ("h2".equals(databaseType)) {
                properties.setProperty("db.url", "jdbc:h2:databases/mah2");
            } else {
                throw new IllegalStateException("Unknown database type: " + databaseType);
            }

            properties.setProperty("db.nosql.enabled", Boolean.toString("ias-tsdb".equals(implementation)));
            properties.setProperty("db.tsnext.enabled", Boolean.toString("tsl".equals(implementation)));

            properties.setProperty("db.tsnext.type", "memory");
            properties.setProperty("db.tsnext.memory.seriesValueLimit", "-1");
            properties.setProperty("db.tsnext.batchInsert.enable", "false");

            // disable batch size; so it doesn't take forever to delete point values from SQL on lifecycle terminate
            properties.setProperty("db.batchSize", "-1");

            // load the NoSQL module defs
            loadModules();

            // add this instance as a bean so our parameters can be used in the configuration
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(getClass());
            beanDefinition.setInstanceSupplier(() -> this);
            beanDefinition.setScope(ConfigurableBeanFactory.SCOPE_SINGLETON);

            lifecycle.addBeanDefinition("tsdbMockMango", beanDefinition);
        }

        @Setup(Level.Trial)
        public void setup() {
            this.pvDao = Common.getBean(PointValueDao.class);
        }

        @Override
        public void setupTrial(SetSecurityContext setSecurityContext) throws Exception {
            if ("mysql".equals(databaseType)) {
                this.jdbcContainer = new MySQLContainer<>(DockerImageName.parse("mysql").withTag("5.7.34"));
            }
            if (jdbcContainer != null) {
                jdbcContainer.start();
            }
            super.setupTrial(setSecurityContext);
        }

        @Override
        public void tearDownTrial() throws IOException, SQLException {
            super.tearDownTrial();
            if (jdbcContainer != null) {
                jdbcContainer.stop();
            }
        }
    }

}
