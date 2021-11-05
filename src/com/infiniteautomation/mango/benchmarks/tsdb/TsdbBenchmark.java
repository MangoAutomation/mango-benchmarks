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
import java.util.Objects;

import org.checkerframework.checker.nullness.qual.NonNull;
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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.util.unit.DataSize;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.infiniteautomation.mango.benchmarks.MockMango;
import com.serotonin.m2m2.Common;
import com.serotonin.m2m2.db.dao.PointValueDao;
import com.serotonin.m2m2.db.dao.SystemSettingsDao;

public abstract class TsdbBenchmark {
    public static final String THREADS_PARAM = "threads";
    public static final String POINTS_PARAM = "points";
    public static final String BATCH_SIZE_PARAM = "batchSize";
    public static final Collection<String> DEFAULT_THREADS = Collections.singletonList("1C");
    public static final Collection<String> DEFAULT_POINTS = Collections.singletonList("1000");
    public static final Collection<String> DEFAULT_BATCH_SIZE = Collections.singletonList("1000");
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

        int[] batchSizeParams = options.getParameter(BATCH_SIZE_PARAM)
                .orElse(DEFAULT_BATCH_SIZE)
                .stream()
                .mapToInt(Integer::parseInt).toArray();

        for (int threads : threadsParams) {
            for (int points : pointsParams) {
                for (int batchSize : batchSizeParams) {
                    var builder = new OptionsBuilder()
                            .parent(options)
                            .threads(threads)
                            .operationsPerInvocation((points / threads) * batchSize)
                            .param(THREADS_PARAM, Integer.toString(threads))
                            .param(POINTS_PARAM, Integer.toString(points))
                            .param(BATCH_SIZE_PARAM, Integer.toString(batchSize));

                    if (options.getIncludes().isEmpty()) {
                        builder.include(getClass().getName());
                    }

                    var opts = builder.build();
                    results.addAll(new Runner(opts).run());
                }
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

        @Param({"sql:h2", "sql:mysql", "ias-tsdb", "tsl:memory", "tsl:quest", "tsl:timescale", "tsl:clickhouse"})
        String implementation;

        /**
         * This default is overridden via {@link TsdbBenchmark#DEFAULT_THREADS}
         */
        @Param({"1"})
        int threads;

        /**
         * Number of points to insert/read values for, the points are split between the threads.
         * So if you set points to 1000 and threads to 10, then each thread will operate on a set of 100 points.
         * This default is overridden via {@link TsdbBenchmark#DEFAULT_POINTS}.
         */
        @Param({"1"})
        int points;

        /**
         * Number of point values which are inserted/read per point.
         * This default is overridden via {@link TsdbBenchmark#DEFAULT_BATCH_SIZE}.
         */
        @Param({"1"})
        int batchSize;

        @Param({"2X"})
        String maxOpenFiles;

//        @Param({"INPUT_STREAM", "FILE_CHANNEL", "RANDOM_ACCESS_FILE", "MAPPED_BYTE_BUFFER"})
        @Param({"MAPPED_BYTE_BUFFER"})
        String shardStreamType;

        @Param("false")
        boolean disableContainers;

        @Param("5000") //ms
        long period;

        @Param("1970-01-01T00:00:00.000Z")
        String startDate;

        PointValueDao pvDao;
        JdbcDatabaseContainer<?> jdbcContainer;

        public static class BenchmarkConfig {
            @Bean
            public BeanPostProcessor postProcessor() {
                return new BeanPostProcessor() {
                    @Override
                    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
                        if (bean instanceof SystemSettingsDao) {
                            // prevents setRetentionPolicy() being called on the PointValueDao
                            // this is required since we insert data at epoch 0, i.e. 1970
                            ((SystemSettingsDao) bean).setIntValue(SystemSettingsDao.POINT_DATA_PURGE_PERIODS, 0);
                        }
                        return bean;
                    }
                };
            }
        }

        @Override
        protected void preInitialize() {
            int maxOpenFiles = parseMultiplier(this.maxOpenFiles, "X", points);
            properties.setProperty("db.nosql.maxOpenFiles", Integer.toString(maxOpenFiles));
            properties.setProperty("db.nosql.shardStreamType", shardStreamType);

            properties.setProperty("db.nosql.enabled", Boolean.toString(implementation.equals("ias-tsdb")));

            // run h2 disk database by default, overridden below
            properties.setProperty("db.type", "h2");
            properties.setProperty("db.url", "jdbc:h2:databases/mah2");

            var parts = implementation.split(":");
            if (implementation.startsWith("sql:")) {
                properties.setProperty("db.type", parts[1]);
                if (jdbcContainer != null) {
                    properties.setProperty("db.url", jdbcContainer.getJdbcUrl());
                    properties.setProperty("db.username", jdbcContainer.getUsername());
                    properties.setProperty("db.password", jdbcContainer.getPassword());
                }
            } else if (implementation.startsWith("tsl:")) {
                String prefix = "db.tsl." + parts[1] + ".";
                properties.setProperty(prefix + "enabled", Boolean.toString(true));
                properties.setProperty(prefix + "batchInsert.enable", "false");
                properties.setProperty("db.tsl.memory.seriesValueLimit", "-1");
                if (jdbcContainer != null) {
                    properties.setProperty(prefix + "host", jdbcContainer.getHost());
                    properties.setProperty(prefix + "db", jdbcContainer.getDatabaseName());
                    properties.setProperty(prefix + "username", jdbcContainer.getUsername());
                    properties.setProperty(prefix + "password", jdbcContainer.getPassword());
                    properties.setProperty(prefix + "port", Integer.toString(jdbcContainer.getFirstMappedPort()));
                }
            } else if (!implementation.equals("ias-tsdb")) {
                throw new IllegalStateException("Unknown implementation: " + implementation);
            }

            properties.setProperty("internal.monitor.diskUsage.enabled", "false");
            properties.setProperty("internal.monitor.enableOperatingSystemInfo", "false");

            // disable batch delete size; so it doesn't take forever to delete point values from SQL on lifecycle terminate
            properties.setProperty("db.batchDeleteSize", "-1");
            // don't event bother deleting point values after test, containers are terminated, temp directory is removed
            properties.setProperty("tests.after.deleteAllPointData", "false");
            properties.setProperty("db.batchSize", "1000");

            // load the NoSQL module defs
            loadModules();

            // add this instance as a bean so our parameters can be used in the configuration
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(getClass());
            beanDefinition.setInstanceSupplier(() -> this);
            beanDefinition.setScope(ConfigurableBeanFactory.SCOPE_SINGLETON);

            lifecycle.addBeanDefinition("tsdbMockMango", beanDefinition);
            lifecycle.addRuntimeContextConfiguration(BenchmarkConfig.class);
        }

        @Setup(Level.Trial)
        public void setup() {
            this.pvDao = Common.getBean(PointValueDao.class);
        }

        @Override
        public void setupTrial(SetSecurityContext setSecurityContext) throws Exception {
            if (!disableContainers) {
                switch (implementation) {
                    case "sql:mysql":
                        this.jdbcContainer = new MySQLContainer<>(DockerImageName.parse("mysql").withTag("5.7.36"));
                        break;
                    case "tsl:clickhouse":
                        this.jdbcContainer = new ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server").withTag("21.8.10.19")) {
                            @Override
                            public String getDatabaseName() {
                                return "default";
                            }
                        };
                        break;
                    case "tsl:timescale":
                        this.jdbcContainer = new PostgreSQLContainer<>(DockerImageName.parse("timescale/timescaledb").withTag("2.4.2-pg13")
                                .asCompatibleSubstituteFor("postgres"))
                                .withCreateContainerCmdModifier(cmd -> Objects.requireNonNull(cmd.getHostConfig())
                                        .withShmSize(DataSize.ofGigabytes(1).toBytes()));
                        break;
                }
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
