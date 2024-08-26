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

/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPOutputStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ManualIntegrationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(ManualIntegrationTest.class);

    private SparkSession spark = null;

    private final String s3endpoint = "http://127.0.0.1:48080";
    private final String s3identity = "s3identity";
    private final String s3credential = "s3credential";

    private final MockS3 mockS3 = new MockS3(s3endpoint, s3identity, s3credential);

    private final MockDBData mockDBData = new MockDBData();

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

        //expectedRows = preloadS3Data(); // for archive only
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
                    .option("S3endPoint", s3endpoint)
                    .option("S3identity", s3identity)
                    .option("S3credential", s3credential)
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
                    .option("S3endPoint", s3endpoint)
                    .option("S3identity", s3identity)
                    .option("S3credential", s3credential)
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

    private long preloadS3Data() throws IOException {
        long rows = 0L;
        AmazonS3 amazonS3 = new Pth06S3Client(s3endpoint, s3identity, s3credential).build();

        TreeMap<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> virtualDatabaseMap = mockDBData
                .getVirtualDatabaseMap();

        for (
            Map.Entry<Long, Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> entry : virtualDatabaseMap
                    .entrySet()
        ) {
            Iterator<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> it = entry
                    .getValue()
                    .iterator();
            while (it.hasNext()) {
                // id, directory, stream, host, logtag, logdate, bucket, path, logtime, filesize
                Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record10 = it
                        .next();
                Long id = record10.get(0, ULong.class).longValue();
                String directory = record10.get(1, String.class);
                String stream = record10.get(2, String.class);
                String host = record10.get(3, String.class);
                String logtag = record10.get(4, String.class);
                Date logdate = record10.get(5, Date.class);
                String bucket = record10.get(6, String.class);
                String path = record10.get(7, String.class);
                Long logtime = record10.get(8, ULong.class).longValue();
                Long filesize = record10.get(9, ULong.class).longValue();

                String recordAsJson = record10.formatJSON();

                // <46>1 2010-01-01T12:34:56.123456+02:00 hostname.domain.tld pstats - -
                SyslogMessage syslog = new SyslogMessage();
                syslog = syslog
                        .withFacility(Facility.USER)
                        .withSeverity(Severity.WARNING)
                        .withTimestamp(logtime)
                        .withHostname(host)
                        .withAppName(logtag)
                        .withMsg(recordAsJson);

                // [event_id@48577 hostname="hostname.domain.tld" uuid="" unixtime="" id_source="source"]

                SDElement event_id_48577 = new SDElement("event_id@48577")
                        .addSDParam("hostname", host)
                        .addSDParam("uuid", UUID.randomUUID().toString())
                        .addSDParam("source", "source")
                        .addSDParam("unixtime", Long.toString(System.currentTimeMillis()));

                syslog = syslog.withSDElement(event_id_48577);

                // [event_format@48577 original_format="rfc5424"]

                SDElement event_format_48577 = new SDElement("event_id@48577").addSDParam("original_format", "rfc5424");

                syslog = syslog.withSDElement(event_format_48577);

                // [event_node_relay@48577 hostname="relay.domain.tld" source="hostname.domain.tld" source_module="imudp"]

                SDElement event_node_relay_48577 = new SDElement("event_node_relay@48577")
                        .addSDParam("hostname", "relay.domain.tld")
                        .addSDParam("source", host)
                        .addSDParam("source_module", "imudp");

                syslog = syslog.withSDElement(event_node_relay_48577);

                // [event_version@48577 major="2" minor="2" hostname="relay.domain.tld" version_source="relay"]

                SDElement event_version_48577 = new SDElement("event_version@48577")
                        .addSDParam("major", "2")
                        .addSDParam("minor", "2")
                        .addSDParam("hostname", "relay.domain.tld")
                        .addSDParam("version_source", "relay");

                syslog = syslog.withSDElement(event_version_48577);

                // [event_node_router@48577 source="relay.domain.tld" source_module="imrelp" hostname="router.domain.tld"]

                SDElement event_node_router_48577 = new SDElement("event_node_router@48577")
                        .addSDParam("source", "relay.domain.tld")
                        .addSDParam("source_module", "imrelp")
                        .addSDParam("hostname", "router.domain.tld");

                syslog = syslog.withSDElement(event_node_router_48577);

                // [origin@48577 hostname="original.hostname.domain.tld"]

                SDElement origin_48577 = new SDElement("origin@48577")
                        .addSDParam("hostname", "original.hostname.domain.tld");
                syslog = syslog.withSDElement(origin_48577);

                // check if this bucket exists
                boolean bucketExists = false;
                for (Bucket existingBucket : amazonS3.listBuckets()) {
                    if (existingBucket.getName().equals(bucket)) {
                        bucketExists = true;
                        break;
                    }
                }
                if (!bucketExists) {
                    amazonS3.createBucket(bucket);
                }

                // compress the message
                String syslogMessage = syslog.toRfc5424SyslogMessage();

                ByteArrayOutputStream outStream = new ByteArrayOutputStream(syslogMessage.length());
                GZIPOutputStream gzip = new GZIPOutputStream(outStream);
                gzip.write(syslogMessage.getBytes());
                gzip.close();

                ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

                // upload as file
                amazonS3.putObject(bucket, path, inStream, null);
                rows++;
            }

        }
        return rows;
    }
}
