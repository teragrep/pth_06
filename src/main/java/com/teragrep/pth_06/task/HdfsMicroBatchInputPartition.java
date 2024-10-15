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

import com.teragrep.pth_06.HdfsFileMetadata;
import com.teragrep.pth_06.config.HdfsConfig;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class HdfsMicroBatchInputPartition implements InputPartition {

    public final Logger LOGGER = LoggerFactory.getLogger(HdfsMicroBatchInputPartition.class);

    public final LinkedList<HdfsFileMetadata> taskObjectList;

    public final long includeRecordEpochAndAfter; // Represents the cutoff epoch which dictates which files should not be fetched from HDFS based on their timestamps.
    public final String hdfsPath; // Represents the working directory path in HDFS filesystem.
    public final String hdfsUri; // Represents the address of the HDFS server.
    public final String useHdfsHostname;
    public final String hdfsTransferProtection;
    public final String hdfsCipherSuites;
    public final boolean useMockHdfsDatabase; // Represents the configuration parameter if mock database should be used or not. Used only for testing.
    public final String kerberosAuthentication;
    public final String kerberosAuthorization;
    public final String kerberosPrincipalPattern;
    public final String kerberosKdc;
    public final String kerberosRealm;
    public final String kerberosKeytabUser;
    public final String kerberosKeytabPath;
    public final String kerberosAutorenewal;

    public HdfsMicroBatchInputPartition(HdfsConfig hdfsConfig, LinkedList<HdfsFileMetadata> hdfsTaskList) {
        this.taskObjectList = hdfsTaskList;
        kerberosAutorenewal = hdfsConfig.kerberosAutorenewal;
        hdfsUri = hdfsConfig.hdfsUri;
        hdfsPath = hdfsConfig.hdfsPath;
        includeRecordEpochAndAfter = hdfsConfig.includeRecordEpochAndAfter;
        useHdfsHostname = hdfsConfig.useHdfsHostname;
        hdfsTransferProtection = hdfsConfig.hdfsTransferProtection;
        hdfsCipherSuites = hdfsConfig.hdfsCipherSuites;
        useMockHdfsDatabase = hdfsConfig.useMockHdfsDatabase;
        kerberosAuthentication = hdfsConfig.kerberosAuthentication;
        kerberosAuthorization = hdfsConfig.kerberosAuthorization;
        kerberosPrincipalPattern = hdfsConfig.kerberosPrincipalPattern;
        kerberosKdc = hdfsConfig.kerberosKdc;
        kerberosRealm = hdfsConfig.kerberosRealm;
        kerberosKeytabUser = hdfsConfig.kerberosKeytabUser;
        kerberosKeytabPath = hdfsConfig.kerberosKeytabPath;
    }

}
