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

import com.teragrep.pth_06.planner.MockDBRowSource;
import com.teragrep.pth_06.task.s3.MockS3;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.StringReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class EpochMigrationTest {

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

        totalRows = Assertions.assertDoesNotThrow(mockS3DataProvider::preloadS3Data);
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
        final List<Long> epochMillisList = new ArrayList<>();
        int loops = 0;
        int syslogCount = 0;
        for (final Row row : rows) {
            final Timestamp timestamp = row.getAs("_time");
            final long epochMillis = timestamp.getTime();
            epochMillisList.add(epochMillis);

            // test JSON contained in the _raw column
            final String raw = row.getAs("_raw");
            Assertions.assertNotNull(raw);
            Assertions.assertFalse(raw.isEmpty());

            try (final JsonReader reader = Json.createReader(new StringReader(raw))) {
                final JsonObject json = reader.readObject();

                Assertions.assertTrue(json.getBoolean("epochMigration"));
                Assertions
                        .assertTrue(json.getString("format").equals("rfc5424") || json.getString("format").equals("non-rfc5424"));

                final JsonObject object = json.getJsonObject("object");
                Assertions.assertEquals("hundred-year", object.getString("bucket"));

                final String path = object.getString("path");
                Assertions
                        .assertTrue(path.matches("^.*2010/\\d{2}-\\d{2}/sc-99-99-14-\\d+/f17_v2/f17_v2\\.logGLOB.*$"), "Path did not match expected epoch migration JSON format: " + path);

                final JsonObject jsonTimestamp = json.getJsonObject("timestamp");
                final String timestampSource = jsonTimestamp.getString("source");
                if (timestampSource.equals("syslog")) {
                    Assertions.assertEquals("syslog", timestampSource);
                    Assertions.assertEquals(epochMillis * 1000, jsonTimestamp.getJsonNumber("epoch").longValue());
                    syslogCount++;
                }
                else {
                    Assertions.assertEquals("non-syslog", timestampSource);
                    jsonTimestamp.isNull("epoch");
                    Assertions.assertTrue(jsonTimestamp.isNull("epoch"));
                }
                final long pathExtractedEpoch = jsonTimestamp.getJsonNumber("path-extracted").longValue();
                Assertions.assertEquals(epochMillis * 1000 * 1000, pathExtractedEpoch);
            }

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
            loops++;
        }
        Assertions.assertEquals(33, syslogCount);
        Assertions.assertEquals(33, loops);
        final List<Long> expectedEpochs = Arrays
                .asList(
                        1262296800L, 1262300400L, 1262383200L, 1262386800L, 1262469600L, 1262473200L, 1262556000L,
                        1262559600L, 1262642400L, 1262646000L, 1262728800L, 1262732400L, 1262815200L, 1262818800L,
                        1262901600L, 1262905200L, 1262988000L, 1262991600L, 1263074400L, 1263078000L, 1263160800L,
                        1263164400L, 1263247200L, 1263250800L, 1263333600L, 1263337200L, 1263420000L, 1263423600L,
                        1263506400L, 1263510000L, 1263592800L, 1263596400L, 1263679200L
                );
        Assertions.assertEquals(expectedEpochs, epochMillisList);
    }
}
