/*
 * Copyright (C) 2021 Radix IoT LLC. All rights reserved.
 */

package com.infiniteautomation.mango.benchmarks.tsdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.infiniteautomation.mango.benchmarks.MockMango;
import com.infiniteautomation.mango.spring.DatabaseProxyConfiguration;
import com.serotonin.m2m2.Common;
import com.serotonin.m2m2.MockMangoProperties;
import com.serotonin.m2m2.db.DatabaseProxy;
import com.serotonin.m2m2.db.DatabaseProxyFactory;
import com.serotonin.m2m2.db.DatabaseType;
import com.serotonin.m2m2.db.H2InMemoryDatabaseProxy;
import com.serotonin.m2m2.db.PointValueDaoDefinition;
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

    public static class BenchmarkConfig {
        @Primary
        @Bean
        public PointValueDao pointValueDao(TsdbMockMango mango, List<PointValueDaoDefinition> defs) {
            var className = "com.serotonin.m2m2.module.definitions.db.DefaultPointValueDaoDefinition";
            if ("ias-tsdb".equals(mango.implementation)) {
                className = "com.infiniteautomation.nosql.MangoNoSqlPointValueDaoDefinition";
            } else if (!"sql".equals(mango.implementation)) {
                throw new UnsupportedOperationException();
            }

            final var classNameFinal = className;

            PointValueDaoDefinition def = defs.stream()
                    .filter(d -> d.getClass().getName().equals(classNameFinal))
                    .findFirst()
                    .orElseThrow();
            return def.getPointValueDao();
        }

        @Bean
        @Primary
        public DatabaseProxy databaseProxy(TsdbMockMango mango, DatabaseProxyFactory factory, DatabaseProxyConfiguration configuration) {
            if ("h2:memory".equals(mango.databaseType)) {
                return new H2InMemoryDatabaseProxy(configuration);
            } else {
                return factory.createDatabaseProxy(DatabaseType.valueOf(mango.databaseType.toUpperCase()));
            }
        }
    }

    public static class TsdbMockMango extends MockMango {
        //@Param({"h2:memory", "h2"})
        @Param({"h2"})
        String databaseType;

        @Param({"ias-tsdb", "sql"})
        String implementation;

        @Param({"1"})
        int threads;

        @Param({"1"})
        int points;

        @Param({"2X"})
        String maxOpenFiles;

//        @Param({"INPUT_STREAM", "FILE_CHANNEL", "RANDOM_ACCESS_FILE", "MAPPED_BYTE_BUFFER"})
        @Param({"MAPPED_BYTE_BUFFER"})
        String shardStreamType;

        PointValueDao pvDao;

        @Override
        protected void preInitialize() {
            MockMangoProperties props = (MockMangoProperties) Common.envProps;
            int maxOpenFiles = parseMultiplier(this.maxOpenFiles, "X", points);
            props.setProperty("db.nosql.maxOpenFiles", Integer.toString(maxOpenFiles));
            props.setProperty("db.nosql.shardStreamType", shardStreamType);

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

        @Setup
        public void setup() {
            this.pvDao = Common.getBean(PointValueDao.class);
        }
    }

}