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

import com.teragrep.pth_06.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.jooq.Record11;
import org.jooq.Result;
import org.jooq.types.ULong;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Collection;

/**
 * Provides access to hbase logfile table
 */
public final class LogfileTable {

    // TODO find more elegant way to store a shared connection
    private final Configuration configuration;
    private final Config config;
    private static Connection connection = null;

    public LogfileTable(final Config config) {
        this(config.hBaseConfig.asHadoopConfig(), config);
    }

    public LogfileTable(final Configuration configuration, final Config config) {
        this.configuration = configuration;
        this.config = config;
    }

    public Connection connection() {
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            }
            catch (IOException e) {
                throw new RuntimeException("Error getting hbase connection: " + e.getMessage());
            }
        }
        return connection;
    }

    public void close() {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            }
            catch (IOException e) {
                throw new RuntimeException("Error closing hbase connection: " + e.getMessage());
            }
        }
        connection = null;
    }

    public Table table() {
        final TableName tableName = TableName.valueOf(config.hBaseConfig.tableName);
        try (Admin admin = connection().getAdmin()) {
            // create logfile table if missing
            if (!admin.tableExists(tableName)) {
                final ColumnFamilyDescriptor metaColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("meta"))
                        .setMaxVersions(3)
                        .build();
                final ColumnFamilyDescriptor bloomColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("bloom"))
                        .setMaxVersions(3)
                        .build();
                final TableDescriptor tableDescriptor = TableDescriptorBuilder
                        .newBuilder(tableName)
                        .setColumnFamily(metaColumnFamilyDescriptor)
                        .setColumnFamily(bloomColumnFamilyDescriptor)
                        .build();

                admin.createTable(tableDescriptor);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error getting HBase table <" + tableName + ">: " + e.getMessage());
        }

        try {
            return connection().getTable(tableName);
        }
        catch (IOException e) {
            throw new RuntimeException("Error getting logfile table: " + e.getMessage());
        }
    }

    public void insertResults(
            Collection<Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>>> dataMap
    ) throws IOException {

        Table table = table();

        for (
            Result<Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong>> result : dataMap
        ) {

            for (
                Record11<ULong, String, String, String, String, Date, String, String, Long, ULong, ULong> record : result
            ) {
                long id = record.get(0, ULong.class).longValue();
                String directory = record.get(1, String.class);
                String stream = record.get(2, String.class);
                String host = record.get(3, String.class);
                String logtag = record.get(4, String.class);
                Date logdate = record.get(5, Date.class);
                String bucket = record.get(6, String.class);
                String path = record.get(7, String.class);
                Long logtime = record.get(8, Long.class);
                long filesize = record.get(9, ULong.class).longValue();
                ULong uncompressedFilesize = record.get(10, ULong.class);

                ByteBuffer rowKeyBuffer = ByteBuffer.allocate(Long.BYTES * 3);
                rowKeyBuffer.putLong(1L); // expects stream_id of 1
                rowKeyBuffer.putLong(logtime);
                rowKeyBuffer.putLong(id);

                Put put = new Put(rowKeyBuffer.array());
                byte[] family = Bytes.toBytes("meta");
                put.addColumn(family, Bytes.toBytes("i"), Bytes.toBytes(id));
                put.addColumn(family, Bytes.toBytes("d"), Bytes.toBytes(directory));
                put.addColumn(family, Bytes.toBytes("s"), Bytes.toBytes(stream));
                put.addColumn(family, Bytes.toBytes("h"), Bytes.toBytes(host));
                put.addColumn(family, Bytes.toBytes("lt"), Bytes.toBytes(logtag));
                put.addColumn(family, Bytes.toBytes("ld"), Bytes.toBytes(logdate.toString())); // as String
                put.addColumn(family, Bytes.toBytes("b"), Bytes.toBytes(bucket));
                put.addColumn(family, Bytes.toBytes("p"), Bytes.toBytes(path));
                put.addColumn(family, Bytes.toBytes("t"), Bytes.toBytes(logtime));
                put.addColumn(family, Bytes.toBytes("fs"), Bytes.toBytes(filesize));
                if (uncompressedFilesize == null) {
                    put.addColumn(family, Bytes.toBytes("ufs"), new byte[0]);
                }
                else {
                    put.addColumn(family, Bytes.toBytes("ufs"), Bytes.toBytes(uncompressedFilesize.longValue()));
                }

                table.put(put);
            }
        }
    }
}
