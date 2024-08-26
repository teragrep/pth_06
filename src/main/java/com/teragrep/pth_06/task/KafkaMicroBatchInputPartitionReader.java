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

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.pth_06.task.kafka.KafkaRecordConverter;
import com.teragrep.pth_06.planner.MockKafkaConsumerFactory;
import com.teragrep.rlo_06.ParseException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * <h1>Kafka Micro Batch Input Partition Reader</h1> Micro batch reader for kafka partition data source.
 *
 * @see PartitionReader
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaMicroBatchInputPartitionReader implements PartitionReader<InternalRow> {

    final Logger LOGGER = LoggerFactory.getLogger(KafkaMicroBatchInputPartitionReader.class);

    private Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordsIterator = Collections.emptyIterator();

    private final KafkaRecordConverter kafkaRecordConverter;
    private final Consumer<byte[], byte[]> kafkaConsumer;

    private final long endOffset;
    private long currentOffset;
    private InternalRow currentRow;
    private final long includeRowsAtAndAfterEpochMicros;
    private final boolean skipNonRFC5424Records;

    private Instant startTime;
    private long dataLength;

    public KafkaMicroBatchInputPartitionReader(
            Map<String, Object> executorKafkaProperties,
            TopicPartition topicPartition,
            long startOffset,
            long endOffset,
            Map<String, String> executorConfig,
            boolean skipNonRFC5424Records
    ) {
        this.skipNonRFC5424Records = skipNonRFC5424Records;

        startTime = Instant.now();

        // print out execution
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            hostname = "";
        }

        LOGGER
                .info(
                        "KafkaMicroBatchInputPartitionReader instantiated for" + " topic <" + topicPartition.topic()
                                + ">" + " partition <" + topicPartition.partition() + ">" + " on host <" + hostname
                                + ">"
                );

        this.kafkaRecordConverter = new KafkaRecordConverter();

        if (
            executorConfig.containsKey("useMockKafkaConsumer")
                    && "true".equals(executorConfig.get("useMockKafkaConsumer"))
        ) {
            LOGGER.warn("useMockKafkaConsumer is set, initialized MockKafkaConsumer");
            this.kafkaConsumer = MockKafkaConsumerFactory.getConsumer();
        }
        else {
            this.kafkaConsumer = new KafkaConsumer<>(executorKafkaProperties);
            this.kafkaConsumer.assign(Collections.singletonList(topicPartition));
        }

        this.endOffset = endOffset;
        LOGGER.debug("startOffset: " + startOffset + ", " + "endOffset: " + endOffset);
        this.kafkaConsumer.seek(topicPartition, startOffset);

        // set cut-off time
        includeRowsAtAndAfterEpochMicros = Math
                .multiplyExact(Long.parseLong(executorConfig.get("includeEpochAndAfter")), 1000L * 1000L);
    }

    @VisibleForTesting
    public KafkaMicroBatchInputPartitionReader(
            Consumer<byte[], byte[]> kafkaConsumer,
            TopicPartition topicPartition,
            long startOffset,
            long endOffset
    ) {

        this.skipNonRFC5424Records = false;

        this.kafkaRecordConverter = new KafkaRecordConverter();

        this.kafkaConsumer = kafkaConsumer;
        this.endOffset = endOffset;

        this.currentOffset = startOffset;
        LOGGER.debug("@VisibleForTesting " + "startOffset: " + startOffset + ", " + "endOffset: " + endOffset);
        this.kafkaConsumer.seek(topicPartition, startOffset);

        // set cut-off time
        includeRowsAtAndAfterEpochMicros = Math.multiplyExact(Long.MIN_VALUE / (1000 * 1000), 1000L * 1000L);
    }

    @Override
    public boolean next() {
        boolean rv = false;

        while (currentOffset < endOffset - 1) { // end exclusive
            while (!kafkaRecordsIterator.hasNext()) {
                // still need to consume more, infinitely loop because connection problems may cause return of an empty iterator
                try {
                    ConsumerRecords<byte[], byte[]> kafkaRecords = kafkaConsumer.poll(Duration.ofSeconds(60)); // TODO parametrize
                    if (kafkaRecords.isEmpty()) {
                        LOGGER.warn("kafkaRecords empty after poll, will retry.");
                    }
                    kafkaRecordsIterator = kafkaRecords.iterator();
                    LOGGER.debug("polled at currentOffset: " + currentOffset + " endOffset: " + endOffset);
                }
                catch (TopicAuthorizationException topicAuthorizationException) {
                    // not authorized to access this topic, bailing out
                    LOGGER.warn("Task triggered TopicAuthorizationException: " + topicAuthorizationException);
                    return false;
                }
            }

            ConsumerRecord<byte[], byte[]> consumerRecord = kafkaRecordsIterator.next();

            currentOffset = consumerRecord.offset(); // update current

            try {
                currentRow = convertToRow(consumerRecord);
            }
            catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
            catch (ParseException parseException) {
                LOGGER
                        .warn(
                                "Kafka Record at offset: " + currentOffset + " produced an exception while parsing: "
                                        + parseException
                        );
                // skip
                if (skipNonRFC5424Records) {
                    continue;
                }
                else {
                    throw parseException;
                }

            }

            // do the time based inclusion
            long rfc5424time = currentRow.getLong(0); // timestamp as epochMicros
            if (rfc5424time >= includeRowsAtAndAfterEpochMicros) {
                rv = true;
                break;
            }
        }

        LOGGER.debug("next rv: " + rv);
        return rv;
    }

    private InternalRow convertToRow(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException {
        dataLength = dataLength + consumerRecord.serializedValueSize();
        // kafka provided data
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        String topic = consumerRecord.topic();
        byte[] key = consumerRecord.key();
        byte[] value = consumerRecord.value();

        // process rfc5424 provided data
        ByteArrayInputStream payload = new ByteArrayInputStream(value);

        return kafkaRecordConverter.convert(payload, String.valueOf(partition), offset);
    }

    @Override
    public InternalRow get() {
        LOGGER.debug("get(): " + currentOffset);
        return currentRow;
    }

    @Override
    public void close() {
        kafkaConsumer.close(Duration.ofSeconds(60)); // TODO parametrize

        long timeTaken = Instant.now().getEpochSecond() - startTime.getEpochSecond();
        LOGGER
                .warn(
                        "KafkaMicroBatchInputPartitionReader took <" + timeTaken + ">" + " for processing dataLength <"
                                + dataLength + ">"
                );
    }
}
