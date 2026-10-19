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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class KafkaQueryImpl implements KafkaQuery {

    private final Config config;
    private final Topics<TopicPartition> partitions;
    private final Consumer<byte[], byte[]> consumer;
    private final boolean isContinuouslyProcessing;

    public KafkaQueryImpl(final Config config) {
        this(config, new KafkaSubscriptionPatternFromQuery(config.query).pattern());
    }

    public KafkaQueryImpl(final Config config, Pattern pattern) {
        this(
                config,
                new CombinedTopicPartitions(
                        new RegexMatchingKafkaTopics(
                                new TopicsFromKafka(new KafkaConsumer<>(config.kafkaConfig.driverOpts)),
                                pattern
                        ),
                        new KafkaConsumer<>(config.kafkaConfig.driverOpts)
                ),
                new KafkaConsumer<>(config.kafkaConfig.driverOpts),
                config.kafkaConfig.kafkaContinuousProcessing
        );
    }

    public KafkaQueryImpl(
            final Config config,
            final Topics<TopicPartition> partitions,
            final Consumer<byte[], byte[]> consumer,
            final boolean isContinuouslyProcessing
    ) {
        this.config = config;
        this.partitions = partitions;
        this.consumer = consumer;
        this.isContinuouslyProcessing = isContinuouslyProcessing;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets() {
        final List<TopicPartition> returnedPartitions;

        if (isContinuouslyProcessing) {
            final Pattern pattern = new KafkaSubscriptionPatternFromQuery(config.query).pattern();
            final CombinedTopicPartitions currentPartitions = new CombinedTopicPartitions(
                    new RegexMatchingKafkaTopics(new TopicsFromKafka(consumer), pattern),
                    consumer
            );
            // new snapshot
            returnedPartitions = currentPartitions.asList();
        }
        else {
            returnedPartitions = partitions.asList();
        }
        return consumer.endOffsets(returnedPartitions, Duration.ofSeconds(60));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets() {
        return consumer.beginningOffsets(partitions.asList(), Duration.ofSeconds(60));
    }

}
