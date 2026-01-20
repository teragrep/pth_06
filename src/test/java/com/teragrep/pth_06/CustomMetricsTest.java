/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06;

import com.teragrep.pth_06.planner.*;
import com.teragrep.pth_06.task.s3.MockS3;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;
import scala.collection.JavaConverters;

import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class CustomMetricsTest {

    private SparkSession spark;

    private final MockS3Configuration mockS3Configuration = new MockS3Configuration(
            "http://127.0.0.1:48080",
            "s3identity",
            "s3credential"
    );

    private final MockS3 mockS3 = new MockS3(
            mockS3Configuration.s3endpoint(),
            mockS3Configuration.s3identity(),
            mockS3Configuration.s3credential()
    );
    private final MockS3DataProvider mockS3DataProvider = new MockS3DataProvider(
            new MockDBRowSource(),
            mockS3Configuration
    );
    private long expectedRows = 0L;

    @BeforeAll
    public void prepareEnv() {
        Assertions.assertDoesNotThrow(mockS3::start);

        spark = SparkSession
                .builder()
                .appName("teragrep")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.metrics.namespace", "teragrep")
                .getOrCreate();

        expectedRows = Assertions.assertDoesNotThrow(mockS3DataProvider::preloadS3Data)
                + MockKafkaConsumerFactory.getNumRecords();
    }

    @AfterAll
    public void decommissionEnv() {
        Assertions.assertDoesNotThrow(mockS3::stop);
    }

    @Test
    public void testDriverCustomMetrics() {
        final SQLAppStatusStore statusStore = spark.sharedState().statusStore();
        final int oldCount = statusStore.executionsList().size();

        final Dataset<Row> df = spark
                .readStream()
                .format("com.teragrep.pth_06.MockTeragrepDatasource")
                .option("archive.enabled", "true")
                .option("S3endPoint", mockS3Configuration.s3endpoint())
                .option("S3identity", mockS3Configuration.s3identity())
                .option("S3credential", mockS3Configuration.s3credential())
                .option("DBusername", "mock")
                .option("DBpassword", "mock")
                .option("DBurl", "mock")
                .option("DBstreamdbname", "mock")
                .option("DBjournaldbname", "mock")
                .option("num_partitions", "1")
                .option("queryXML", "<index value=\"f17\" operation=\"EQUALS\"/>")
                // audit information
                .option("TeragrepAuditQuery", "index=f17")
                .option("TeragrepAuditReason", "test run at fullScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "true")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();

        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());

        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());

        // Metrics
        final Map<String, List<Long>> metricsValues = new HashMap<>();

        while (statusStore.executionsCount() <= oldCount) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        while (statusStore.executionsList().isEmpty() || statusStore.executionsList().last().metricValues() == null) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        statusStore.executionsList(oldCount, (int) (statusStore.executionsCount() - oldCount)).foreach(v1 -> {
            final Map<Object, String> mv = JavaConverters.mapAsJavaMap(v1.metricValues());
            for (final SQLPlanMetric spm : JavaConverters.asJavaIterable(v1.metrics())) {
                final long id = spm.accumulatorId();
                final Object value = mv.get(id);
                if (spm.metricType().startsWith("v2Custom_") && value != null) {
                    final List<Long> preExistingValues = metricsValues
                            .getOrDefault(spm.metricType(), new ArrayList<>());
                    preExistingValues.add(Long.parseLong(value.toString()));
                    metricsValues.put(spm.metricType(), preExistingValues);
                }
            }
            return 0; // need to return something, expects scala Unit return value
        });

        final long expectedArchiveRows = 33L;
        // Check that all expected metrics are present
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator"
                                ),
                        "ArchiveOffset metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey("v2Custom_com.teragrep.pth_06.metrics.offsets.KafkaOffsetMetricAggregator"),
                        "KafkaOffset metric not present!"
                );

        // Get minimum and maximum archive offsets, and assert them
        Assertions
                .assertEquals(
                        expectedArchiveRows, metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator")
                                .size()
                );
        final long maxArchiveOffset = metricsValues
                .get("v2Custom_com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator")
                .stream()
                .max(Long::compare)
                .orElseGet(Assertions::fail);
        final long minArchiveOffset = metricsValues
                .get("v2Custom_com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator")
                .stream()
                .min(Long::compare)
                .orElseGet(Assertions::fail);
        Assertions.assertEquals(1263679200L, maxArchiveOffset);
        Assertions.assertEquals(1262296800L, minArchiveOffset);

        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        new HashSet<>(
                                metricsValues
                                        .get(
                                                "v2Custom_com.teragrep.pth_06.metrics.offsets.ArchiveOffsetMetricAggregator"
                                        )
                        ).size()
                );

        // kafka offsets
        Assertions
                .assertEquals(
                        expectedArchiveRows, metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.offsets.KafkaOffsetMetricAggregator")
                                .size()
                );
        // all kafka offsets the same (in unit tests all kafka data is retrieved in first batch from 0->14 offset)
        Assertions
                .assertEquals(
                        1,
                        new HashSet<>(
                                metricsValues
                                        .get("v2Custom_com.teragrep.pth_06.metrics.offsets.KafkaOffsetMetricAggregator")
                        ).size()
                );
        Assertions
                .assertEquals(
                        1L, metricsValues.get("v2Custom_com.teragrep.pth_06.metrics.offsets.KafkaOffsetMetricAggregator").get(0)
                );
    }

    @Test
    public void testPartitionReaderCustomMetrics() {
        final SQLAppStatusStore statusStore = spark.sharedState().statusStore();
        final int oldCount = statusStore.executionsList().size();

        final Dataset<Row> df = spark
                .readStream()
                .format("com.teragrep.pth_06.MockTeragrepDatasource")
                .option("archive.enabled", "true")
                .option("S3endPoint", mockS3Configuration.s3endpoint())
                .option("S3identity", mockS3Configuration.s3identity())
                .option("S3credential", mockS3Configuration.s3credential())
                .option("DBusername", "mock")
                .option("DBpassword", "mock")
                .option("DBurl", "mock")
                .option("DBstreamdbname", "mock")
                .option("DBjournaldbname", "mock")
                .option("num_partitions", "1")
                .option("queryXML", "<index value=\"f17\" operation=\"EQUALS\"/>")
                // audit information
                .option("TeragrepAuditQuery", "index=f17")
                .option("TeragrepAuditReason", "test run at fullScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "true")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();

        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());

        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());

        // Metrics
        final Map<String, List<Long>> metricsValues = new HashMap<>();

        while (statusStore.executionsCount() <= oldCount) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        while (statusStore.executionsList().isEmpty() || statusStore.executionsList().last().metricValues() == null) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        statusStore.executionsList(oldCount, (int) (statusStore.executionsCount() - oldCount)).foreach(v1 -> {
            final Map<Object, String> mv = JavaConverters.mapAsJavaMap(v1.metricValues());
            for (final SQLPlanMetric spm : JavaConverters.asJavaIterable(v1.metrics())) {
                final long id = spm.accumulatorId();
                final Object value = mv.get(id);
                if (spm.metricType().startsWith("v2Custom_") && value != null) {
                    final List<Long> preExistingValues = metricsValues
                            .getOrDefault(spm.metricType(), new ArrayList<>());
                    preExistingValues.add(Long.parseLong(value.toString()));
                    metricsValues.put(spm.metricType(), preExistingValues);
                }
            }
            return 0; // need to return something, expects scala Unit return value
        });

        // Check that all expected metrics are present
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.bytes.ArchiveCompressedBytesProcessedMetricAggregator"
                                ),
                        "ArchiveCompressedBytesProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesProcessedMetricAggregator"),
                        "BytesProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesPerSecondMetricAggregator"),
                        "BytesPerSecond metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator"
                                ),
                        "RecordsProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.RecordsPerSecondMetricAggregator"
                                ),
                        "RecordsPerSecond metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.objects.ArchiveObjectsProcessedMetricAggregator"
                                ),
                        "ArchiveObjectsProcessed metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.records.LatestKafkaTimestampMetricAggregator"
                                ),
                        "LatestKafkaTimestamp metric not present!"
                );

        final long expectedArchiveRows = 33L;
        final long expectedKafkaRows = 14L;
        final long expectedTotalRows = expectedArchiveRows + expectedKafkaRows;
        // 33 batches, 33 values for each metric
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.bytes.ArchiveCompressedBytesProcessedMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows, metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesProcessedMetricAggregator")
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.objects.ArchiveObjectsProcessedMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator")
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedTotalRows,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsProcessedMetricAggregator")
                                .stream()
                                .reduce(Long::sum)
                                .orElseGet(Assertions::fail)
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.records.RecordsPerSecondMetricAggregator")
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows, metricsValues
                                .get("v2Custom_com.teragrep.pth_06.metrics.bytes.BytesPerSecondMetricAggregator")
                                .size()
                );
    }

    @Test
    public void testDatabaseCustomMetrics() {
        final SQLAppStatusStore statusStore = spark.sharedState().statusStore();
        final int oldCount = statusStore.executionsList().size();

        final Dataset<Row> df = spark
                .readStream()
                .format("com.teragrep.pth_06.MockTeragrepDatasource")
                .option("archive.enabled", "true")
                .option("S3endPoint", mockS3Configuration.s3endpoint())
                .option("S3identity", mockS3Configuration.s3identity())
                .option("S3credential", mockS3Configuration.s3credential())
                .option("DBusername", "mock")
                .option("DBpassword", "mock")
                .option("DBurl", "mock")
                .option("DBstreamdbname", "mock")
                .option("DBjournaldbname", "mock")
                .option("num_partitions", "1")
                .option("queryXML", "<index value=\"f17\" operation=\"EQUALS\"/>")
                // audit information
                .option("TeragrepAuditQuery", "index=f17")
                .option("TeragrepAuditReason", "test run at fullScanTest()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "true")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .load();

        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());

        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());

        // Metrics
        final Map<String, List<Long>> metricsValues = new HashMap<>();

        while (statusStore.executionsCount() <= oldCount) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        while (statusStore.executionsList().isEmpty() || statusStore.executionsList().last().metricValues() == null) {
            Assertions.assertDoesNotThrow(() -> Thread.sleep(100));
        }

        statusStore.executionsList(oldCount, (int) (statusStore.executionsCount() - oldCount)).foreach(v1 -> {
            final Map<Object, String> mv = JavaConverters.mapAsJavaMap(v1.metricValues());
            for (final SQLPlanMetric spm : JavaConverters.asJavaIterable(v1.metrics())) {
                final long id = spm.accumulatorId();
                final Object value = mv.get(id);
                if (spm.metricType().startsWith("v2Custom_") && value != null) {
                    final List<Long> preExistingValues = metricsValues
                            .getOrDefault(spm.metricType(), new ArrayList<>());
                    preExistingValues.add(Long.parseLong(value.toString()));
                    metricsValues.put(spm.metricType(), preExistingValues);
                }
            }
            return 0; // need to return something, expects scala Unit return value
        });

        // Check that all expected metrics are present
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMaxLatencyMetricAggregator"
                                ),
                        "ArchiveDatabaseRowMaxLatency metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowAvgLatencyMetricAggregator"
                                ),
                        "ArchiveDatabaseRowAvgLatency metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMinLatencyMetricAggregator"
                                ),
                        "ArchiveDatabaseRowMinLatency metric not present!"
                );
        Assertions
                .assertTrue(
                        metricsValues
                                .containsKey(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowCountMetricAggregator"
                                ),
                        "ArchiveDatabaseRowCount metric not present!"
                );

        // database latency metrics
        final long expectedArchiveRows = 33L;
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMaxLatencyMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowAvgLatencyMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowMinLatencyMetricAggregator"
                                )
                                .size()
                );
        Assertions
                .assertEquals(
                        expectedArchiveRows,
                        metricsValues
                                .get(
                                        "v2Custom_com.teragrep.pth_06.metrics.database.ArchiveDatabaseRowCountMetricAggregator"
                                )
                                .size()
                );
    }

}
