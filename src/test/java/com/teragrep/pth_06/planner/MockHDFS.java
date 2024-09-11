package com.teragrep.pth_06.planner;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
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
    public String startMiniCluster() throws IOException, InterruptedException {

        // Create a HDFS miniCluster
        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        // System.out.println("hdfsURI: " + hdfsURI);
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();

        insertMockFiles();

        return hdfsURI;
    }

    // Teardown the minicluster
    public void teardownMiniCluster() {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    public static void insertMockFiles() throws IOException {
        String path = hdfsPath + "testConsumerTopic" ; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
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

        String dir = System.getProperty("user.dir")+"/src/test/java/com/teragrep/pth_06/mockHdfsFiles";
        Set<String> listOfFiles = Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
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
            if (!avroFile.getName().equals("0.13")) {
                fs.copyFromLocalFile(readPath, hdfswritepath);
            }
            LOGGER.debug("End Write file into hdfs");
            LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());
        }
        fs.close();
    }

    public void simpleTest() {
        try {
            hdfsReadTest();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void hdfsReadTest() throws IOException {
        Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
        // Check that the files were properly written to HDFS with a read test.
        String path = "hdfs:///opt/teragrep/cfe_39/srv/"; // This path parameter should only lead to the directory that holds the directories representing different Kafka topics.
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
        Path workingDir=fs.getWorkingDirectory();
        Path newDirectoryPath= new Path(path);
        if(!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.info("Path {} created.", path);
        }
        FileStatus[] directoryStatuses = fs.listStatus(newDirectoryPath, pathFilter);
        if (directoryStatuses.length > 0) {
            for (FileStatus r : directoryStatuses) {
                LOGGER.info("The path to the directory is: {}", r.getPath());
                LOGGER.info("The directory name is: {}", r.getPath().getName());
                FileStatus[] fileStatuses = fs.listStatus(r.getPath());
                for (FileStatus r2 : fileStatuses) {
                    String topic = r2.getPath().getParent().getName();
                    String[] split = r2.getPath().getName().split("\\."); // The file name can be split to partition parameter and offset parameter. First value is partition and second is offset.
                    String partition = split[0];
                    String offset = split[1];
                    LOGGER.info("File belongs to topic: {}", topic);
                    LOGGER.info("File belongs to partition: {}", partition);
                    LOGGER.info("File has an offset of: {}", offset);
                    if (!hdfsStartOffsets.containsKey(new TopicPartition(topic, Integer.parseInt(partition)))) {
                        hdfsStartOffsets.put(new TopicPartition(topic, Integer.parseInt(partition)), Long.parseLong(offset));
                    } else {
                        if (hdfsStartOffsets.get(new TopicPartition(topic, Integer.parseInt(partition))) < Long.parseLong(offset)) {
                            hdfsStartOffsets.replace(new TopicPartition(topic, Integer.parseInt(partition)), Long.parseLong(offset));
                        }
                    }
                }
            }
        }else {
            LOGGER.info("No matching directories found");
        }
        LOGGER.info("hdfsStartOffsets.toString(): ");
        LOGGER.info(hdfsStartOffsets.toString());

        LinkedList<String> rv = new LinkedList<>();
        JsonArray ja = new JsonArray();
        // Serializing the hdfsStartOffsets to a JsonArray ja.
        for (Map.Entry<TopicPartition, Long> entry : hdfsStartOffsets.entrySet()) {
            String topic = entry.getKey().topic();
            String partition = String.valueOf(entry.getKey().partition());
            String offset = String.valueOf(entry.getValue());
            rv.add(String.format(
                    "{\"topic\":\"%s\", \"partition\":\"%s\", \"offset\":\"%s\"}",
                    topic, partition, offset
            ));
            JsonObject jo = new JsonObject(); // TODO: Use this instead of string
            jo.addProperty("topic", topic);
            jo.addProperty("partition", partition);
            jo.addProperty("offset", offset);
            ja.add(jo);
        }
        // LOGGER.info(rv.toString());
        LOGGER.info("ja.toString(): ");
        LOGGER.info(ja.toString());

        // Deserialize ja back to Map<TopicPartition, Long>
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        for (JsonElement pa : ja) {
            JsonObject offsetObject  = pa.getAsJsonObject();
            TopicPartition topicPartition = new TopicPartition(offsetObject.get("topic").getAsString(), offsetObject.get("partition").getAsInt());
            Long offset = offsetObject.get("offset").getAsLong();
            offsetMap.put(topicPartition, offset);
        }

        LOGGER.info("offsetMap.toString(): ");
        LOGGER.info(offsetMap.toString());
        // Assert that hdfsStartOffsets and offsetMap are identical after the json serialization and deserialization cycle of hdfsStartOffsets.
        Assertions.assertEquals(hdfsStartOffsets, offsetMap);

        // ja.toString() now outputs:
        // [{"topic":"testConsumerTopic","partition":"7","offset":"13"},{"topic":"testConsumerTopic","partition":"8","offset":"13"},{"topic":"testConsumerTopic","partition":"5","offset":"13"},{"topic":"testConsumerTopic","partition":"6","offset":"13"},{"topic":"testConsumerTopic","partition":"3","offset":"13"},{"topic":"testConsumerTopic","partition":"4","offset":"13"},{"topic":"testConsumerTopic","partition":"1","offset":"13"},{"topic":"testConsumerTopic","partition":"2","offset":"13"},{"topic":"testConsumerTopic","partition":"0","offset":"13"},{"topic":"testConsumerTopic","partition":"9","offset":"13"}]
        fs.close();
    }

    private static final PathFilter pathFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return path.getName().matches("^testConsumer.*$"); // Catches the directory names.
        }
    };

}
