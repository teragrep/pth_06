/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth06;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * <h2>Archive Source Provider</h2>
 *
 * Class for representing a structured source for accessing Spark data source.
 *
 * @see DataSourceRegister
 * @see MicroBatchReadSupport
 * @see DataSourceV2
 * @since 02/03/2021
 * @author Mikko Kortelainen
 */
public class ArchiveSourceProvider implements
        DataSourceRegister,
        MicroBatchReadSupport,
        DataSourceV2 {

    final Logger LOGGER = LoggerFactory.getLogger(ArchiveSourceProvider.class);

    public static final StructType Schema = new StructType(
        new StructField[]{
            new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
            new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
            new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
            new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build()),
        }
    );

    @Override
    public String shortName() {
        return "archive";
    }

	/**
	 * Public constructor for creating configured micro batch reader.
	 *
	 * @param optional Optional schema that is used if present, otherwise uses default
	 * @param metadataPath String path for metadata
	 * @param dataSourceOptions Spark data source options
	 * @return {@link com.teragrep.pth06.ArchiveMicroBatchReader}
	 */
    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> optional,
                                                   String metadataPath,
                                                   DataSourceOptions dataSourceOptions) {
        Config config = new Config(dataSourceOptions);

        StructType schema = Schema;

        if (optional.isPresent()) {
            // override default Schema
            schema = optional.get();
			LOGGER.info("Use custom Schema");
        }

        return new ArchiveMicroBatchReader(schema, config);
    }


}
