package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.HdfsTopicPartitionOffsetMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

// Searches the HDFS database for files contained in directories based on topicsRegexString. Creates a list of objects where each object contains the metadata of a single HDFS-file.
public class HdfsDBClient {

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsDBClient.class);
    private final String path;
    private final FileSystem fs;
    private final Configuration conf;
    private final String hdfsuri;
    private final String topicsRegexString;
    private final long ignoreBeforeEpoch;

    public HdfsDBClient(Config config, String topicsRegexString) throws IOException {
        this.topicsRegexString = topicsRegexString;
        this.ignoreBeforeEpoch = config.hdfsConfig.cutoffOffset; // Defines the minimum time window / cutoff epoch for making sure that the files which metadata is fetched by the planner are not mistakenly deleted by cfe_39 pruning before tasker can process the records inside.
        // Implement HDFS FileSystem access here
        if (config.hdfsConfig.useMockHdfsDatabase) {
            // Code for initializing the class in test mode without kerberos.
            hdfsuri = config.hdfsConfig.hdfsUri; // Get from config.

            // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
            // In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
            // These values should be fetched from config and other input parameters (topic+partition+offset).
            path = config.hdfsConfig.hdfsPath; // Example path: hdfs://localhost:34509/opt/teragrep/cfe_39/srv/

            // ====== Init HDFS File System Object
            conf = new Configuration();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsuri);
            // Because of Maven
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            // filesystem for HDFS access is set here
            try {
                fs = FileSystem.get(URI.create(hdfsuri), conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else {
            // Code for initializing the class with kerberos.
            hdfsuri = config.hdfsConfig.hdfsUri; // Get from config.

            // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
            // In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
            // The values are fetched from config and input parameters (topic+partition+offset).
            path = config.hdfsConfig.hdfsPath;


            // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
            // System.setProperty("HADOOP_USER_NAME", "hdfs"); // Not needed because user authentication is done by kerberos?
            // System.setProperty("hadoop.home.dir", "/"); // Not needed because user authentication is done by kerberos?

            // TODO: Add all the Kerberos parameters to Config

            // set kerberos host and realm
            //System.setProperty("java.security.krb5.realm", config.getKerberosRealm());
            //System.setProperty("java.security.krb5.kdc", config.getKerberosHost());

            conf = new Configuration();

            // enable kerberus
            //conf.set("hadoop.security.authentication", config.getHadoopAuthentication());
            //conf.set("hadoop.security.authorization", config.getHadoopAuthorization());

            conf.set("fs.defaultFS", hdfsuri); // Set FileSystem URI
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName()); // Maven stuff?
            conf.set("fs.file.impl", LocalFileSystem.class.getName()); // Maven stuff?

            // hack for running locally with fake DNS records
            // set this to true if overriding the host name in /etc/hosts
            //conf.set("dfs.client.use.datanode.hostname", config.getKerberosTestMode());

            // server principal
            // the kerberos principle that the namenode is using
            //conf.set("dfs.namenode.kerberos.principal.pattern", config.getKerberosPrincipal());

            // set usergroup stuff
            UserGroupInformation.setConfiguration(conf);
            //UserGroupInformation.loginUserFromKeytab(config.getKerberosKeytabUser(), config.getKerberosKeytabPath());

            // filesystem for HDFS access is set here
            fs = FileSystem.get(conf);
        }
    }

    // this queries and pulls the distinct file metadata values to the partitionList according to the given query conditions (condition only applies to topic names in planner side).
    public LinkedList<HdfsTopicPartitionOffsetMetadata> pullToPartitionList() throws IOException {
        LinkedList<HdfsTopicPartitionOffsetMetadata> rv = new LinkedList<>();
        // path holds the fileSystem path to the directory that holds a collection of other directories, each different directory representing a different topic.
        FileStatus[] directoryStatuses = fs.listStatus(new Path(path), topicFilter);
        // If the path only holds one directory, fileStatuses will only hold one FileStatus object which returns this value when fileStatus.getPath() is called:
        // hdfs://localhost:34509/opt/teragrep/cfe_39/srv/testConsumerTopic

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
                    HdfsTopicPartitionOffsetMetadata temp = new HdfsTopicPartitionOffsetMetadata(new TopicPartition(topic, Integer.parseInt(partition)), Integer.parseInt(offset), r2.getPath().toString(), r2.getLen());
                    // Add the HdfsTopicPartitionOffsetMetadata object to the rv only if the file's modification timestamp is above ignoreBeforeEpoch. Timestamps are in milliseconds.
                    if (r2.getModificationTime() >= ignoreBeforeEpoch) {
                        rv.add(temp);
                    }
                }
            }
        }else {
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