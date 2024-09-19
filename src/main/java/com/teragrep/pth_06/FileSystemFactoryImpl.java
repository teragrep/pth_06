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

import com.teragrep.pth_06.config.HdfsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public final class FileSystemFactoryImpl implements FileSystemFactory {

    private final String kerberosAuthentication;
    private final String hdfsUri;
    private final String kerberosRealm;
    private final String kerberosKdc;
    private final String kerberosAuthorization;
    private final String kerberosAutorenewal;
    private final String UseHdfsHostname;
    private final String kerberosPrincipalPattern;
    private final String hdfsTransferProtection;
    private final String hdfsCipherSuites;
    private final String kerberosKeytabUser;
    private final String kerberosKeytabPath;

    public FileSystemFactoryImpl(HdfsConfig config) {
        this(
                config.kerberosAuthentication,
                config.hdfsUri,
                config.kerberosRealm,
                config.kerberosKdc,
                config.kerberosAuthorization,
                config.kerberosAutorenewal,
                config.UseHdfsHostname,
                config.kerberosPrincipalPattern,
                config.hdfsTransferProtection,
                config.hdfsCipherSuites,
                config.kerberosKeytabUser,
                config.kerberosKeytabPath
        );
    }

    public FileSystemFactoryImpl(
            String kerberosAuthentication,
            String hdfsUri,
            String kerberosRealm,
            String kerberosKdc,
            String kerberosAuthorization,
            String kerberosAutorenewal,
            String UseHdfsHostname,
            String kerberosPrincipalPattern,
            String hdfsTransferProtection,
            String hdfsCipherSuites,
            String kerberosKeytabUser,
            String kerberosKeytabPath
    ) {
        this.kerberosAuthentication = kerberosAuthentication;
        this.hdfsUri = hdfsUri;
        this.kerberosRealm = kerberosRealm;
        this.kerberosKdc = kerberosKdc;
        this.kerberosAuthorization = kerberosAuthorization;
        this.kerberosAutorenewal = kerberosAutorenewal;
        this.UseHdfsHostname = UseHdfsHostname;
        this.kerberosPrincipalPattern = kerberosPrincipalPattern;
        this.hdfsTransferProtection = hdfsTransferProtection;
        this.hdfsCipherSuites = hdfsCipherSuites;
        this.kerberosKeytabUser = kerberosKeytabUser;
        this.kerberosKeytabPath = kerberosKeytabPath;
    }

    @Override
    public FileSystem create(boolean initializeUGI) throws IOException {
        FileSystem fs;
        if ("kerberos".equals(kerberosAuthentication)) {
            // Code for initializing the FileSystem with kerberos.

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

            if (initializeUGI) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(kerberosKeytabUser, kerberosKeytabPath);
            }

            // filesystem for HDFS access is set here
            fs = FileSystem.get(conf);
        }
        else {
            // Code for initializing the FileSystem without kerberos.

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
        return fs;
    }
}
