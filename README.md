# Mango benchmarks
This module can be used to run benchmarks for Mango.

## Build and prepare for running
`mvn package dependency:copy-dependencies`

## Run a benchmark
Windows

```shell
java -cp "maven-target\mango-benchmarks-4.3.0-SNAPSHOT.jar;maven-target\dependency\*" com.infiniteautomation.mango.benchmarks.tsdb.Insert
```

Linux

```shell
java -cp "maven-target/mango-benchmarks-4.3.0-SNAPSHOT.jar:maven-target/dependency/*" com.infiniteautomation.mango.benchmarks.tsdb.Insert
```

## Setting parameters and benchmark options

Use [JHM command line syntax](https://github.com/guozheng/jmh-tutorial/blob/master/README.md) to set parameters and benchmark options.
You can set multiple options for each parameter and every combination will be benchmarked.
Example:

```shell
java -cp <cp> com.infiniteautomation.mango.benchmarks.tsdb.Insert -p threads=1C -p points=100,1000
```

### Available parameters

#### com.infiniteautomation.mango.benchmarks.tsdb.Insert
name | default | options | description
--- | --- | --- | ---
threads | 1C | number > 0 | Number of threads writing to TSDB, use "C" suffix to multiply by number of CPU cores
points | 1000 | number > 0 | Number of data points, use "C" suffix to multiply by number of CPU cores
databaseType | h2 | h2, h2:mem, mysql | SQL database type
implementation | sql, ias-tsdb | sql, ias-tsdb | PointValueDao implementation
maxOpenFiles | 2X | number > 0 | IasTsdb max open files setting, use "X" suffix to multiply by number of points