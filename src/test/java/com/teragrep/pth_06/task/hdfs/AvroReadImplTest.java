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
package com.teragrep.pth_06.task.hdfs;

import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.avro.SyslogRecord;
import com.teragrep.pth_06.planner.MockHDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class AvroReadImplTest {

    private final String hdfsPath = "hdfs:///opt/teragrep/cfe_39/srv/";
    private String hdfsUri; // Can only be defined after starting the mock hdfs.
    private final MockHDFS mockHDFS = new MockHDFS(hdfsPath);

    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        hdfsUri = mockHDFS.startMiniCluster(true);
    }

    @AfterEach
    public void teardown() {
        mockHDFS.teardownMiniCluster();
    }

    @Test
    public void testReadAllRecords() {
        assertDoesNotThrow(() -> {

            // build FileSystem object
            // ====== Init HDFS File System Object
            Configuration conf = new Configuration();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsUri);
            // Because of Maven
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            // filesystem for HDFS access is set here
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

            // build hdfsFileMetadata object
            HdfsFileMetadata testConsumerTopic09 = new HdfsFileMetadata(
                    new TopicPartition("testConsumerTopic", 0),
                    9,
                    hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.9",
                    0
            );
            AvroReadImpl avroReadImpl1 = new AvroReadImpl(fs, testConsumerTopic09);
            long rowNum = 0L;
            while (avroReadImpl1.next()) {
                SyslogRecord syslogRecord = avroReadImpl1.record();
                avroReadImpl1.clear();
                Assertions.assertEquals(rowNum, syslogRecord.getOffset()); // Checks offsets of the consumed records which should range from 0 to 9.
                rowNum++;
            }
            avroReadImpl1.close();
            Assertions.assertEquals(10, rowNum); // Asserts that expected number of records were consumed from the files.

            HdfsFileMetadata testConsumerTopic013 = new HdfsFileMetadata(
                    new TopicPartition("testConsumerTopic", 0),
                    13,
                    hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.13",
                    0
            );
            AvroReadImpl avroReadImpl2 = new AvroReadImpl(fs, testConsumerTopic013);
            while (avroReadImpl2.next()) {
                SyslogRecord syslogRecord = avroReadImpl2.record();
                avroReadImpl2.clear();
                Assertions.assertEquals(rowNum, syslogRecord.getOffset()); // Checks offsets of the consumed records which should range from 10 to 13.
                rowNum++;
            }
            avroReadImpl2.close();
            Assertions.assertEquals(14, rowNum); // Asserts that expected number of records were consumed from the files.
        });
    }

}
