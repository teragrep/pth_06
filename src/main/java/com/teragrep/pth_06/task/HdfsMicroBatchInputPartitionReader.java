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
package com.teragrep.pth_06.task;

import com.teragrep.pth_06.HdfsTopicPartitionOffsetMetadata;
import com.teragrep.pth_06.task.hdfs.HdfsRecordConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

public class HdfsMicroBatchInputPartitionReader implements PartitionReader<InternalRow> {

    private final LinkedList<HdfsTopicPartitionOffsetMetadata> taskObjectList;
    private final FileSystem fs;
    private final long cutoffEpoch;
    private HdfsRecordConverter hdfsRecordConverter;

    public HdfsMicroBatchInputPartitionReader(
            long cutoffEpoch,
            String kerberosAuthentication,
            String hdfsUri,
            String hdfsPath,
            String kerberosRealm,
            String kerberosKdc,
            String kerberosAuthorization,
            String kerberosAutorenewal,
            String UseHdfsHostname,
            String kerberosPrincipalPattern,
            String hdfsTransferProtection,
            String hdfsCipherSuites,
            String kerberosKeytabUser,
            String kerberosKeytabPath,
            LinkedList<HdfsTopicPartitionOffsetMetadata> taskObjectList
    ) throws IOException {
        this.cutoffEpoch = cutoffEpoch;
        this.taskObjectList = taskObjectList;

        // FIXME: Implement FileSystem initialization code somewhere else than the constructor.
        if ("kerberos".equals(kerberosAuthentication)) {
            // Code for initializing the class with kerberos.

            /* The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
             In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
             The values are fetched from config and input parameters (topic+partition+offset).*/

            // set kerberos host and realm
            System.setProperty("java.security.krb5.realm", kerberosRealm);
            System.setProperty("java.security.krb5.kdc", kerberosKdc);

            Configuration conf = new Configuration();

            // enable kerberus
            conf.set("hadoop.security.authentication", kerberosAuthentication);
            conf.set("hadoop.security.authorization", kerberosAuthorization);
            conf.set("hadoop.kerberos.keytab.login.autorenewal.enabled", kerberosAutorenewal);

            conf.set("fs.defaultFS", hdfsUri); // Set FileSystem URI
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName()); // Maven stuff?
            conf.set("fs.file.impl", LocalFileSystem.class.getName()); // Maven stuff?

            // hack for running locally with fake DNS records, set this to true if overriding the host name in /etc/hosts
            conf.set("dfs.client.use.datanode.hostname", UseHdfsHostname);

            // server principal, the kerberos principle that the namenode is using
            conf.set("dfs.namenode.kerberos.principal.pattern", kerberosPrincipalPattern);

            conf.set("dfs.data.transfer.protection", hdfsTransferProtection);
            conf.set("dfs.encrypt.data.transfer.cipher.suites", hdfsCipherSuites);

            // set usergroup stuff
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kerberosKeytabUser, kerberosKeytabPath);

            // filesystem for HDFS access is set here
            fs = FileSystem.get(conf);
        }
        else {
            // Code for initializing the class in test mode without kerberos.

            /* The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
             In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
             These values should be fetched from config and other input parameters (topic+partition+offset).*/

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
            fs = FileSystem.get(URI.create(hdfsUri), conf);
        }
        this.hdfsRecordConverter = new HdfsRecordConverter(fs); // stub
    }

    // Read avro-file until it ends
    @Override
    public boolean next() throws IOException {
        // true if data is available, false if not
        boolean rv = false;

        while (!taskObjectList.isEmpty() && !rv) {
            if (hdfsRecordConverter.isStub()) {
                hdfsRecordConverter = new HdfsRecordConverter(fs, taskObjectList.getFirst());
                hdfsRecordConverter.open();
            }

            rv = hdfsRecordConverter.next();

            if (!rv) {
                // object was consumed
                hdfsRecordConverter.close();

                // remove consumed object
                taskObjectList.removeFirst();
                if (!taskObjectList.isEmpty()) {
                    hdfsRecordConverter = new HdfsRecordConverter(fs, taskObjectList.getFirst());
                    hdfsRecordConverter.open();
                }
            }
            else {
                // time based inclusion, skip record and continue loop if the record is older than cutoffEpoch.
                long rfc5424time = hdfsRecordConverter.get().getLong(0); // timestamp as epochMicros
                if (rfc5424time < cutoffEpoch) {
                    rv = false;
                }
            }
        }
        return rv;
    }

    @Override
    public InternalRow get() {
        return hdfsRecordConverter.get();
    }

    @Override
    public void close() throws IOException {
        if (!hdfsRecordConverter.isStub()) {
            hdfsRecordConverter.close();
        }
    }
}
