package com.teragrep.pth06;

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

import com.teragrep.pth06.planner.*;
import com.teragrep.pth06.scheduler.BatchScheduler;
import com.teragrep.pth06.scheduler.NoOpScheduler;
import com.teragrep.pth06.scheduler.Scheduler;
import com.teragrep.pth06.planner.MockKafkaConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class MockArchiveSourceProvider implements
        DataSourceRegister,
        MicroBatchReadSupport,
        DataSourceV2 {

    @Override
    public String shortName() {
        return "mockArchive";
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> optional,
                                                   String metadataPath,
                                                   DataSourceOptions dataSourceOptions) {
        StructType schema = ArchiveSourceProvider.Schema;

        if (optional.isPresent()) {
            schema = optional.get();
        }

        Config config = new Config(dataSourceOptions);


        ArchiveQuery archiveQueryProcessor = new MockArchiveQueryProcessor("<index operation=\"EQUALS\" value=\"f17_v2\"/>");

        KafkaQuery kafkaQueryProcessor;
        if (config.isKafkaEnabled()) {
            Consumer<byte[], byte[]> kafkaConsumer = MockKafkaConsumerFactory.getConsumer();

            kafkaQueryProcessor =
                    new KafkaQueryProcessor(kafkaConsumer);
        }
        else {
            kafkaQueryProcessor = null;
        }

        // configureable scheduler
        Config.SchedulerType schedulerType = config.getSchedulerType();

        Scheduler scheduler;

        switch (schedulerType) {
            case NOOP:
                scheduler = new NoOpScheduler(archiveQueryProcessor, kafkaQueryProcessor);
                break;
            case BATCH:
                scheduler = new BatchScheduler(archiveQueryProcessor, kafkaQueryProcessor);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + schedulerType);
        }

        return new ArchiveMicroBatchReader(scheduler, schema, config);
    }
}
