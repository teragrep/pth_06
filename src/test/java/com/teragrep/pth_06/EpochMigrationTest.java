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

import com.amazonaws.services.s3.AmazonS3;
import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.SDElement;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.teragrep.pth_06.planner.MockDBData;
import com.teragrep.pth_06.task.s3.MockS3;
import com.teragrep.pth_06.task.s3.Pth06S3Client;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationTest {

    private SparkSession spark;
    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";
    private final MockS3 mockS3 = new MockS3(s3endpoint, s3identity, s3credential);
    long totalRows;

    @BeforeAll
    public void setup() {
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

        totalRows = Assertions.assertDoesNotThrow(this::preloadDualS3Data);
    }

    @AfterAll
    public void stop() {
        Assertions.assertDoesNotThrow(mockS3::stop);
        Assertions.assertDoesNotThrow(spark::close);
    }

    @Test
    public void testEpochMigration() {
        final Dataset<Row> df = spark
                .readStream()
                .format("com.teragrep.pth_06.MockTeragrepDatasource")
                .option("archive.enabled", "true")
                .option("epochMigrationMode", "true")
                .option("S3endPoint", s3endpoint)
                .option("S3identity", s3identity)
                .option("S3credential", s3credential)
                .option("DBusername", "mock")
                .option("DBpassword", "mock")
                .option("DBurl", "mock")
                .option("DBstreamdbname", "mock")
                .option("DBjournaldbname", "mock")
                .option("num_partitions", "1")
                .option("queryXML", "<index value=\"f17\" operation=\"EQUALS\"/>")
                // audit information
                .option("TeragrepAuditQuery", "index=f17")
                .option("TeragrepAuditReason", "testEpochMigration()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                // metadata options
                .option("metadataQuery.enabled", "false")
                .load();
        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());
        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());

        final Dataset<Row> resultDf = spark.sql("SELECT * FROM MockArchiveQuery");
        final long rowCount = resultDf.count();
        Assertions.assertEquals(totalRows, rowCount);

        final List<Row> rows = resultDf.collectAsList();
        for (final Row row : rows) {
            final Timestamp ts = row.getAs("_time");
            final long epochSeconds = ts.getTime();
            // mock data epoch ranges but divided by 1000 when written to S3 so we can ensure epoch is calculated from the s3 object
            Assertions.assertTrue(epochSeconds >= 1200000);
            Assertions.assertTrue(epochSeconds <= 1400000);

            // message should be empty in epoch migration mode
            Assertions.assertEquals("", row.getAs("_raw"));

            // Fields that should have values from the mock data
            Assertions.assertNotNull(row.getAs("index"));
            Assertions.assertFalse(((String) row.getAs("index")).isEmpty(), "index should not be empty");

            Assertions.assertNotNull(row.getAs("source"));
            Assertions.assertFalse(((String) row.getAs("source")).isEmpty(), "source should not be empty");

            Assertions.assertNotNull(row.getAs("origin"));
            Assertions.assertFalse(((String) row.getAs("origin")).isEmpty(), "origin should not be empty");

            Assertions.assertNotNull(row.getAs("sourcetype"));
            Assertions.assertFalse(((String) row.getAs("sourcetype")).isEmpty(), "sourcetype should not be empty");

            Assertions.assertNotNull(row.getAs("host"));
            Assertions.assertFalse(((String) row.getAs("host")).isEmpty(), "host should not be empty");

            Assertions.assertNotNull(row.getAs("partition"));
            Assertions.assertFalse(((String) row.getAs("partition")).isEmpty(), "partition should not be empty");

            final Object offsetObj = row.getAs("offset");
            Assertions.assertNotNull(offsetObj);
            Assertions.assertTrue(offsetObj instanceof Long, "offset should be a Long");
            Assertions.assertEquals(1L, ((Long) offsetObj).longValue(), "offset should always be the first event");

        }
    }

    @Test
    public void testEpochMigrationOptionDisabled() {
        final Dataset<Row> df = spark
                .readStream()
                .format("com.teragrep.pth_06.MockTeragrepDatasource")
                .option("archive.enabled", "true")
                .option("epochMigrationMode", "false")
                .option("S3endPoint", s3endpoint)
                .option("S3identity", s3identity)
                .option("S3credential", s3credential)
                .option("DBusername", "mock")
                .option("DBpassword", "mock")
                .option("DBurl", "mock")
                .option("DBstreamdbname", "mock")
                .option("DBjournaldbname", "mock")
                .option("num_partitions", "1")
                .option("queryXML", "<index value=\"f17\" operation=\"EQUALS\"/>")
                // audit information
                .option("TeragrepAuditQuery", "index=f17")
                .option("TeragrepAuditReason", "testEpochMigration()")
                .option("TeragrepAuditUser", System.getProperty("user.name"))
                // kafka options
                .option("kafka.enabled", "false")
                .option("kafka.bootstrap.servers", "")
                .option("kafka.sasl.mechanism", "")
                .option("kafka.security.protocol", "")
                .option("kafka.sasl.jaas.config", "")
                .option("kafka.useMockKafkaConsumer", "true")
                .option("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                // metadata options
                .option("metadataQuery.enabled", "false")
                .load();
        final StreamingQuery streamingQuery = Assertions
                .assertDoesNotThrow(() -> df.writeStream().outputMode(OutputMode.Append()).format("memory").trigger(Trigger.ProcessingTime(0)).queryName("MockArchiveQuery").option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID()).option("spark.cleaner.referenceTracking.cleanCheckpoints", "true").start());
        streamingQuery.processAllAvailable();
        Assertions.assertDoesNotThrow(streamingQuery::stop);
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
        final Dataset<Row> resultDf = spark.sql("SELECT * FROM MockArchiveQuery");
        final long rowCount = resultDf.count();
        // preloaded s3 data adds 2 messages per S3 object
        Assertions.assertEquals(totalRows * 2, rowCount, "two rows per s3 object should be present");
    }

    private long preloadDualS3Data() throws IOException {
        long rows = 0L;
        final AmazonS3 amazonS3 = new Pth06S3Client(s3endpoint, s3identity, s3credential).build();
        final TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = new MockDBData()
                .getVirtualDatabaseMap();
        final Set<String> existingBuckets = new HashSet<>();
        for (
            final Map.Entry<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> entry : virtualDatabaseMap
                    .entrySet()
        ) {
            for (
                final Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record : entry
                        .getValue()
            ) {
                final String bucket = record.get(6, String.class);
                final String path = record.get(7, String.class);

                // add bucket if missing
                if (existingBuckets.add(bucket) && !amazonS3.doesBucketExistV2(bucket)) {
                    amazonS3.createBucket(bucket);
                }

                final String firstEvent = buildSyslogMessage(record);
                final String secondEvent = buildSyslogMessage(record);
                // two messages per s3 object
                final String combined = firstEvent + "\n" + secondEvent;
                final ByteArrayOutputStream baos = new ByteArrayOutputStream(combined.length());
                try (final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
                    gzipOutputStream.write(combined.getBytes(StandardCharsets.UTF_8));
                }
                final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                amazonS3.putObject(bucket, path, bais, null);
                rows++;
            }
        }
        return rows;
    }

    private String buildSyslogMessage(
            final Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record
    ) {
        final String host = record.get(3, String.class);
        final String logtag = record.get(4, String.class);
        // divide by 1000 to test s3 object data is used for epoch values
        final long logtime = record.get(8, ULong.class).longValue() / 1000;
        final String recordAsJson = record.formatJSON();
        final SyslogMessage syslog = new SyslogMessage()
                .withFacility(Facility.USER)
                .withSeverity(Severity.WARNING)
                .withTimestamp(logtime)
                .withHostname(host)
                .withAppName(logtag)
                .withMsg(recordAsJson)
                .withSDElement(new SDElement("event_id@48577").addSDParam("hostname", host).addSDParam("uuid", UUID.randomUUID().toString()).addSDParam("source", "source").addSDParam("unixtime", Long.toString(System.currentTimeMillis()))).withSDElement(new SDElement("event_format@48577").addSDParam("original_format", "rfc5424")).withSDElement(new SDElement("event_node_relay@48577").addSDParam("hostname", "relay.domain.tld").addSDParam("source", host).addSDParam("source_module", "imudp")).withSDElement(new SDElement("event_version@48577").addSDParam("major", "2").addSDParam("minor", "2").addSDParam("hostname", "relay.domain.tld").addSDParam("version_source", "relay")).withSDElement(new SDElement("event_node_router@48577").addSDParam("source", "relay.domain.tld").addSDParam("source_module", "imrelp").addSDParam("hostname", "router.domain.tld")).withSDElement(new SDElement("origin@48577").addSDParam("hostname", "original.hostname.domain.tld"));

        return syslog.toRfc5424SyslogMessage();
    }
}
