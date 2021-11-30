# Mango benchmarks
This module can be used to run benchmarks for Mango.

## Build and prepare for running
1. `mvn clean package`
2. Copy `maven-target/mango-benchmarks.zip` to your target machine
3. `unzip mango-benchmarks.zip`
4. `cd mango-benchmarks`

## Run a benchmark

```shell
java -jar mango-benchmarks.jar 'Insert.insert'
```

## Setting parameters and benchmark options

Use [JHM command line syntax](https://github.com/guozheng/jmh-tutorial/blob/master/README.md) to set parameters and benchmark options.
You can set multiple options for each parameter and every combination will be benchmarked.
Example:

```shell
java -jar mango-benchmarks.jar 'Insert.insert' -p threads=1C -p points=100,1000
```

## Benchmarks and their parameters

### Read benchmark

* Class: `com.infiniteautomation.mango.benchmarks.tsdb.Read`
* Result: op/s represents the total point values read per second, across all points and threads.

#### Parameters

| name                   | default                                                                           | options/format                                                                    | description                                                                                           |
|------------------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| threads                | 1C                                                                                | number > 0                                                                        | Number of threads writing to TSDB, use "C" suffix to multiply by number of CPU cores                  |
| points                 | 1000                                                                              | number > 0                                                                        | Total number of data points, split between threads, use "C" suffix to multiply by number of CPU cores |
| valuesInsertedPerPoint | 10000                                                                             | number > 0                                                                        | Point values inserted per point                                                                       |
| implementation         | sql:h2, sql:mysql, ias-tsdb, tsl:memory, tsl:quest, tsl:timescale, tsl:clickhouse | sql:h2, sql:mysql, ias-tsdb, tsl:memory, tsl:quest, tsl:timescale, tsl:clickhouse | PointValueDao implementation                                                                          |
| batchSize              | 1000                                                                              | number > 0                                                                        | Number of point values to read per point, per iteration                                               |
| period                 | 5000                                                                              | number > 0                                                                        | Period between point values (milliseconds)                                                            |
| startDate              | 1970-01-01T00:00:00.000Z                                                          | ISO 8601                                                                          | Date to start generating data from                                                                    |                                                                                   |                                                                                   |                                                                                                       |
| conflictMode           | UPDATE                                                                            | ERROR, UPDATE, DO_NOTHING                                                         | TSL strategy for handling unique constraint conflicts when inserting                                  |
| maxOpenFiles           | 2X                                                                                | number > 0                                                                        | IasTsdb max open files setting, use "X" suffix to multiply by number of points                        |
| shardStreamType        | MAPPED_BYTE_BUFFER                                                                | INPUT_STREAM, FILE_CHANNEL, RANDOM_ACCESS_FILE, MAPPED_BYTE_BUFFER                | IasTsdb shardStreamType setting                                                                       |

### Insert benchmark

* Class: `com.infiniteautomation.mango.benchmarks.tsdb.Insert`
* Result: op/s represents the total point values inserted per second, across all points and threads.

#### Parameters

| name                   | default                                                                           | options/format                                                                    | description                                                                                           |
|------------------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| threads                | 1C                                                                                | number > 0                                                                        | Number of threads writing to TSDB, use "C" suffix to multiply by number of CPU cores                  |
| points                 | 1000                                                                              | number > 0                                                                        | Total number of data points, split between threads, use "C" suffix to multiply by number of CPU cores |
| implementation         | sql:h2, sql:mysql, ias-tsdb, tsl:memory, tsl:quest, tsl:timescale, tsl:clickhouse | sql:h2, sql:mysql, ias-tsdb, tsl:memory, tsl:quest, tsl:timescale, tsl:clickhouse | PointValueDao implementation                                                                          |
| batchSize              | 1000                                                                              | number > 0                                                                        | Number of point values to insert per point, per iteration                                             |
| period                 | 5000                                                                              | number > 0                                                                        | Period between point values (milliseconds)                                                            |
| startDate              | 1970-01-01T00:00:00.000Z                                                          | ISO 8601                                                                          | Date to start generating data from                                                                    |                                                                                   |                                                                                   |                                                                                                       |
| conflictMode           | UPDATE                                                                            | ERROR, UPDATE, DO_NOTHING                                                         | TSL strategy for handling unique constraint conflicts when inserting                                  |
| maxOpenFiles           | 2X                                                                                | number > 0                                                                        | IasTsdb max open files setting, use "X" suffix to multiply by number of points                        |
| shardStreamType        | MAPPED_BYTE_BUFFER                                                                | INPUT_STREAM, FILE_CHANNEL, RANDOM_ACCESS_FILE, MAPPED_BYTE_BUFFER                | IasTsdb shardStreamType setting                                                                       |

