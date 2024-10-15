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
package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.FileSystemFactoryImpl;
import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.fs.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

// Searches the HDFS database for files contained in directories based on topicsRegexString. Creates a list of objects where each object contains the metadata of a single HDFS-file.
public class HdfsDBClient {

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsDBClient.class);
    private final String path;
    private final FileSystem fs;
    private final String topicsRegexString;
    private final long ignoreBeforeEpoch;

    public HdfsDBClient(Config config, String topicsRegexString) throws IOException {
        this.topicsRegexString = topicsRegexString;
        this.ignoreBeforeEpoch = config.hdfsConfig.includeFileEpochAndAfter; // Defines the minimum time window / cutoff epoch for making sure that the files which metadata is fetched by the planner are not mistakenly deleted by cfe_39 pruning before tasker can process the records inside.
        path = config.hdfsConfig.hdfsPath;
        fs = new FileSystemFactoryImpl(config.hdfsConfig).fileSystem(true);
    }

    // this queries and pulls the distinct file metadata values to the partitionList according to the given query conditions (condition only applies to topic names in planner side).
    public LinkedList<HdfsFileMetadata> pullToPartitionList() throws IOException {
        LinkedList<HdfsFileMetadata> rv = new LinkedList<>();
        // path holds the fileSystem path to the directory that holds a collection of other directories, each different directory representing a different topic.
        FileStatus[] directoryStatuses = fs.listStatus(new Path(path), topicFilter);
        /*If the path only holds one directory, fileStatuses will only hold one FileStatus object which returns this value when fileStatus.getPath() is called:
        hdfs://localhost:34509/opt/teragrep/cfe_39/srv/testConsumerTopic*/

        // Get the directory statuses. Each directory represents a Kafka topic.
        if (directoryStatuses.length > 0) {
            LOGGER.debug("Found {} matching directories", directoryStatuses.length);
            for (FileStatus r : directoryStatuses) {
                // Get the file statuses that are inside the directories.
                FileStatus[] fileStatuses = fs.listStatus(r.getPath());
                for (FileStatus r2 : fileStatuses) {
                    String topic = r2.getPath().getParent().getName();
                    String[] split = r2.getPath().getName().split("\\."); // The file name can be split to partition parameter and offset parameter. First value is partition and second is offset.
                    String partition = split[0];
                    String offset = split[1];
                    HdfsFileMetadata temp = new HdfsFileMetadata(
                            new TopicPartition(topic, Integer.parseInt(partition)),
                            Integer.parseInt(offset),
                            r2.getPath().toString(),
                            r2.getLen()
                    );
                    // Add the HdfsFileMetadata object to the rv only if the file's modification timestamp is above ignoreBeforeEpoch. Timestamps are in milliseconds.
                    if (r2.getModificationTime() >= ignoreBeforeEpoch) {
                        rv.add(temp);
                    }
                }
            }
        }
        else {
            LOGGER.info("No matching directories found");
        }
        return rv;
    }

    private final PathFilter topicFilter = new PathFilter() {

        @Override
        public boolean accept(Path path) {
            return path.getName().matches(topicsRegexString); // Catches the directory names.
        }
    };

}
