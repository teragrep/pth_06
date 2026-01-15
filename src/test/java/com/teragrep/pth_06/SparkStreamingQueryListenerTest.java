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
import com.teragrep.pth_06.planner.MockKafkaConsumerFactory;
import com.teragrep.pth_06.task.s3.MockS3;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;

import java.util.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class SparkStreamingQueryListenerTest {

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
    public void testOnQueryIdle() {
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

        final int[] triggered = {
                0
        };
        final String[] latestOffset = {
                ""
        };
        final UUID id = streamingQuery.id();
        spark.streams().addListener(new StreamingQueryListener() {

            @Override
            public void onQueryStarted(final QueryStartedEvent event) {
                // no-op
            }

            @Override
            public void onQueryProgress(final QueryProgressEvent event) {
                if (id.equals(event.progress().id())) {
                    latestOffset[0] = event.progress().sources()[0].latestOffset();
                }
            }

            @Override
            public void onQueryIdle(final QueryIdleEvent event) {
                if (id.equals(event.id())) {
                    triggered[0]++;
                    Assertions.assertDoesNotThrow(streamingQuery::stop);
                }
            }

            @Override
            public void onQueryTerminated(final QueryTerminatedEvent event) {
                // no-op
            }
        });

        final long expectedRows = 47L;
        Assertions.assertDoesNotThrow(() -> streamingQuery.awaitTermination());
        Assertions.assertEquals(1, triggered[0]);
        Assertions
                .assertEquals(
                        "{\"version\":1,\"archiveOffset\":{\"offset\":1263679200,\"json\":\"1263679200\"},\"kafkaOffset\":{\"serializedKafkaOffset\":{\"testConsumerTopic-0\":14}}}",
                        latestOffset[0]
                );
        Assertions.assertEquals(expectedRows, spark.sql("SELECT COUNT(*) FROM MOCKARCHIVEQUERY;").first().getLong(0));
    }
}
