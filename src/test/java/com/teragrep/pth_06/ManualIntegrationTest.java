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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ManualIntegrationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(ManualIntegrationTest.class);

    private SparkSession spark = null;
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
    public void prepareEnv() throws Exception {
        mockS3.start();

        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[2]")
                .config("spark.driver.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.executor.extraJavaOptions", "-Duser.timezone=EET")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // expectedRows = mockS3DataProvider.preloadS3Data(); // for archive only
    }

    //@Test
    public void testSchedulerNotSpecified() throws StreamingQueryException, TimeoutException {
        fullScanTest(null);
    }

    //@Test
    public void testBatchScheduler() throws StreamingQueryException, TimeoutException {
        fullScanTest("BatchScheduler");
    }

    //@Test
    public void testNoOpScheduler() throws StreamingQueryException, TimeoutException {
        fullScanTest("NoOpScheduler");
    }

    private void fullScanTest(String scheduler) throws StreamingQueryException, TimeoutException {
        // please notice that JAVA_HOME=/usr/lib/jvm/java-1.8.0 mvn clean test -Pdev is required
        Dataset<Row> df;
        if (scheduler == null) {
            df = spark
                    .readStream()
                    .format("com.teragrep.pth_06.TeragrepDatasource")
                    .option("S3endPoint", mockS3Configuration.s3endpoint())
                    .option("S3identity", mockS3Configuration.s3identity())
                    .option("S3credential", mockS3Configuration.s3credential())
                    .option("DBusername", "mock")
                    .option("DBpassword", "mock")
                    .option("DBurl", "mariadb://localhost:3306")
                    .option("DBstreamdbname", "mock")
                    .option("DBjournaldbname", "mock")
                    .option("num_partitions", "1")
                    .option("queryXML", "<index value=\"bulk-data-sorted-3\" operation=\"EQUALS\"/>")
                    // audit information
                    .option("TeragrepAuditQuery", "index=firewall-data")
                    .option("TeragrepAuditReason", "test run at fullScanTest()")
                    .option("TeragrepAuditUser", System.getProperty("user.name"))
                    .option("archive.enabled", "false")
                    // kafka options
                    .option("kafka.enabled", "true")
                    .option(
                            "kafka.bootstrap.servers",
                            "kafkadev01.example.com:9092,kafkadev02.example.com:9092,kafkadev03.example.com:9092"
                    )
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.security.protocol", "SASL_PLAINTEXT")
                    .option(
                            "kafka.sasl.jaas.config",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"superuser\" password=\"SuperSecretSuperuserPassword\";"
                    )
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .load();
        }
        else {
            df = spark
                    .readStream()
                    .format("com.teragrep.pth_06.TeragrepDatasource")
                    .option("S3endPoint", mockS3Configuration.s3endpoint())
                    .option("S3identity", mockS3Configuration.s3identity())
                    .option("S3credential", mockS3Configuration.s3credential())
                    .option("DBusername", "mock")
                    .option("DBpassword", "mock")
                    .option("DBurl", "mariadb://localhost:3306")
                    .option("DBstreamdbname", "mock")
                    .option("DBjournaldbname", "mock")
                    .option("num_partitions", "1")
                    .option("scheduler", scheduler)
                    .option("queryXML", "<index value=\"bulk-data-sorted-3\" operation=\"EQUALS\"/>")
                    // audit information
                    .option("TeragrepAuditQuery", "index=firewall-data")
                    .option("TeragrepAuditReason", "test run at fullScanTest()")
                    .option("TeragrepAuditUser", System.getProperty("user.name"))
                    .option("archive.enabled", "false")
                    // kafka options
                    .option("kafka.enabled", "true")
                    .option(
                            "kafka.bootstrap.servers",
                            "kafkadev01.example.com:9092,kafkadev02.example.com:9092,kafkadev03.example.com:9092"
                    )
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.security.protocol", "SASL_PLAINTEXT")
                    .option(
                            "kafka.sasl.jaas.config",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"superuser\" password=\"SuperSecretSuperuserPassword\";"
                    )
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .load();
        }

        Dataset<Row> df2 = df.agg(functions.count("*"));

        StreamingQuery streamingQuery = df2
                .writeStream()
                .outputMode("complete")
                .format("memory")
                .trigger(Trigger.ProcessingTime(0))
                .queryName("MockArchiveQuery")
                .option("checkpointLocation", "/tmp/checkpoint")
                .start();

        long rowCount = 0;
        while (!streamingQuery.awaitTermination(1000)) {

            long resultSize = spark.sqlContext().sql("SELECT * FROM MockArchiveQuery").count();
            if (resultSize > 0) {
                rowCount = spark.sqlContext().sql("SELECT * FROM MockArchiveQuery").first().getAs(0);
                System.out.println(rowCount);
            }
            if (
                streamingQuery.lastProgress() == null
                        || streamingQuery.status().message().equals("Initializing sources")
            ) {
                // query has not started
            }
            else if (streamingQuery.lastProgress().sources().length != 0) {
                if (isArchiveDone(streamingQuery)) {
                    streamingQuery.stop();
                }
            }
        }

        // CHANGE ME
        Assertions.assertEquals(10, rowCount);
    }

    private boolean isArchiveDone(StreamingQuery outQ) {
        Boolean archiveDone = true;
        for (int i = 0; i < outQ.lastProgress().sources().length; i++) {
            String startOffset = outQ.lastProgress().sources()[i].startOffset();
            String endOffset = outQ.lastProgress().sources()[i].endOffset();
            String description = outQ.lastProgress().sources()[i].description();

            if (description != null && !description.startsWith("com.teragrep.pth06.ArchiveMicroBatchReader@")) {
                // ignore others than archive
                continue;
            }

            if (startOffset != null) {
                if (!startOffset.equalsIgnoreCase(endOffset)) {
                    archiveDone = false;
                }
            }
            else {
                archiveDone = false;
            }
        }
        LOGGER.debug("isArchiveDone(): rv: " + archiveDone);
        return archiveDone;
    }

    @AfterAll
    public void decommissionEnv() throws Exception {
        mockS3.stop();
    }
}
