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
import com.teragrep.pth_06.planner.MockHDFS;
import com.teragrep.pth_06.task.HdfsMicroBatchInputPartitionReader;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class HdfsMicroBatchInputPartitionReaderTest {

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
    public void testHdfsConsumer2Files() {
        assertDoesNotThrow(() -> {
            // create task object list
            LinkedList<HdfsFileMetadata> taskObjectList = new LinkedList<>();
            // Add taskObjects to the taskObjectList according to what files are stored in minicluster during setup.
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 9, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.9", 0));
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 13, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.13", 0));

            HdfsMicroBatchInputPartitionReader hdfsMicroBatchInputPartitionReader = new HdfsMicroBatchInputPartitionReader(
                    0L,
                    "",
                    hdfsUri,
                    hdfsPath,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    taskObjectList
            );

            // read through the files in HDFS
            long rowNum = 0L;
            while (hdfsMicroBatchInputPartitionReader.next()) {
                InternalRow internalRow = hdfsMicroBatchInputPartitionReader.get();
                Assertions.assertEquals(rowNum, internalRow.getLong(7)); // Checks offsets of the consumed records which should range from 0 to 13.
                rowNum++;
            }
            hdfsMicroBatchInputPartitionReader.close();
            Assertions.assertEquals(14, rowNum); // Asserts that expected number of records were consumed from the files.
        });

    }

    @Test
    public void testHdfsConsumer1File() {
        assertDoesNotThrow(() -> {
            // create task object list
            LinkedList<HdfsFileMetadata> taskObjectList = new LinkedList<>();
            // Add only the taskObject related to testConsumerTopic/0.9 file to the taskObjectList.
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 9, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.9", 0));

            HdfsMicroBatchInputPartitionReader hdfsMicroBatchInputPartitionReader = new HdfsMicroBatchInputPartitionReader(
                    0L,
                    "",
                    hdfsUri,
                    hdfsPath,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    taskObjectList
            );

            // read through the files in HDFS
            long rowNum = 0L;
            while (hdfsMicroBatchInputPartitionReader.next()) {
                InternalRow internalRow = hdfsMicroBatchInputPartitionReader.get();
                Assertions.assertEquals(rowNum, internalRow.getLong(7)); // Checks offsets of the consumed records which should range from 0 to 9.
                rowNum++;
            }
            hdfsMicroBatchInputPartitionReader.close();
            Assertions.assertEquals(10, rowNum); // Asserts that expected number of records were consumed from the file.
        });

    }

    @Test
    public void testHdfsConsumer1FileAlt() {
        assertDoesNotThrow(() -> {
            // create task object list
            LinkedList<HdfsFileMetadata> taskObjectList = new LinkedList<>();
            // Add only the taskObject related to testConsumerTopic/0.13 file to the taskObjectList.
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 13, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.13", 0));

            HdfsMicroBatchInputPartitionReader hdfsMicroBatchInputPartitionReader = new HdfsMicroBatchInputPartitionReader(
                    0L,
                    "",
                    hdfsUri,
                    hdfsPath,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    taskObjectList
            );

            // read through the files in HDFS
            long rowNum = 10L;
            while (hdfsMicroBatchInputPartitionReader.next()) {
                InternalRow internalRow = hdfsMicroBatchInputPartitionReader.get();
                Assertions.assertEquals(rowNum, internalRow.getLong(7)); // Checks offsets of the consumed records which should range from 10 to 13.
                rowNum++;
            }
            hdfsMicroBatchInputPartitionReader.close();
            Assertions.assertEquals(14, rowNum);
        });

    }

    @Test
    public void testCutoffEpoch() {
        assertDoesNotThrow(() -> {
            // create task object list
            LinkedList<HdfsFileMetadata> taskObjectList = new LinkedList<>();
            // Add taskObjects to the taskObjectList according to what files are stored in minicluster during setup.
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 9, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.9", 0));
            taskObjectList
                    .add(new HdfsFileMetadata(new TopicPartition("testConsumerTopic", 0), 13, hdfsUri + "opt/teragrep/cfe_39/srv/testConsumerTopic/0.13", 0));

            HdfsMicroBatchInputPartitionReader hdfsMicroBatchInputPartitionReader = new HdfsMicroBatchInputPartitionReader(
                    1650872090804001L, // Offset 0 has timestamp of 1650872090804000L
                    "",
                    hdfsUri,
                    hdfsPath,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    taskObjectList
            );

            // read through the files in HDFS
            long rowNum = 1L; // Offset 0 has timestamp of 1650872090804000L, which means it is filtered out by the time based inclusion.
            while (hdfsMicroBatchInputPartitionReader.next()) {
                InternalRow internalRow = hdfsMicroBatchInputPartitionReader.get();
                Assertions.assertEquals(rowNum, internalRow.getLong(7)); // Checks offsets of the consumed records which should range from 1 to 13.
                rowNum++;
            }
            hdfsMicroBatchInputPartitionReader.close();
            Assertions.assertEquals(14, rowNum);
        });
    }

}
