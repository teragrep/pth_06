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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MockHDFS {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockHDFS.class);
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static String hdfsURI;
    private static String hdfsPath;

    public MockHDFS(String hdfsPath) {
        MockHDFS.hdfsPath = hdfsPath; // "hdfs:///opt/teragrep/cfe_39/srv/"
    }

    // Start minicluster and initialize config. Returns the hdfsUri of the minicluster.
    public String startMiniCluster(boolean insertAll) throws IOException, InterruptedException {

        // Create a HDFS miniCluster
        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        // System.out.println("hdfsURI: " + hdfsURI);
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();

        insertMockFiles(insertAll);

        return hdfsURI;
    }

    // Teardown the minicluster
    public void teardownMiniCluster() {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    public static void insertMockFiles(boolean insertAll) throws IOException {
        String path = hdfsPath + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsURI);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsURI), conf);

        //==== Create directory if not exists
        Path workingDir = fs.getWorkingDirectory();
        // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
        Path newDirectoryPath = new Path(path);
        if (!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.debug("Path {} created.", path);
        }

        String dir = System.getProperty("user.dir") + "/src/test/java/com/teragrep/pth_06/mockHdfsFiles";
        Set<String> listOfFiles = Stream
                .of(Objects.requireNonNull(new File(dir).listFiles()))
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
        // Loop through all the avro files
        for (String fileName : listOfFiles) {
            String pathname = dir + "/" + fileName;
            File avroFile = new File(pathname);
            //==== Write file
            LOGGER.debug("Begin Write file into hdfs");
            //Create a path
            Path hdfswritepath = new Path(newDirectoryPath + "/" + avroFile.getName()); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
            if (fs.exists(hdfswritepath)) {
                throw new RuntimeException("File " + avroFile.getName() + " already exists");
            }
            Path readPath = new Path(avroFile.getPath());
            /* This condition can be used for managing the test offsets between HDFS and Kafka. If 0.13 file is included in the HDFS, then Kafka won't have any new records available for reading.
              If 0.13 is excluded, then records from 0 to 8 will be read from HDFS and records from 9 to 13 will be read from Kafka.
              Implement a method to decide if all the files should be added to HDFS or not*/
            if (insertAll) {
                fs.copyFromLocalFile(readPath, hdfswritepath);
            }
            else {
                if (!avroFile.getName().equals("0.13")) {
                    fs.copyFromLocalFile(readPath, hdfswritepath);
                }
            }
            LOGGER.debug("End Write file into hdfs");
            LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());
        }
        fs.close();
    }
}
