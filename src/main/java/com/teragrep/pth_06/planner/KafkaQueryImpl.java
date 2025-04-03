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
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class KafkaQueryImpl implements KafkaQuery {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueryImpl.class);

    private final KafkaSubscriptionPatternFromQuery patternFromQuery;
    private final Consumer<byte[], byte[]> consumer;
    private final Map<TopicPartition, Long> persistedEndOffsetMap; // used as a cache when not processing continuously
    private final boolean isContinuouslyProcessing;

    public KafkaQueryImpl(final Config config) {
        this(
                new KafkaSubscriptionPatternFromQuery(config.query),
                new KafkaConsumer<>(config.kafkaConfig.driverOpts),
                new HashMap<>(),
                config.kafkaConfig.kafkaContinuousProcessing
        );
    }

    public KafkaQueryImpl(
            final KafkaSubscriptionPatternFromQuery patternFromQuery,
            final Consumer<byte[], byte[]> consumer,
            final Map<TopicPartition, Long> persistedEndOffsetMap,
            final boolean isContinuouslyProcessing
    ) {
        this.patternFromQuery = patternFromQuery;
        this.consumer = consumer;
        this.persistedEndOffsetMap = persistedEndOffsetMap;
        this.isContinuouslyProcessing = isContinuouslyProcessing;
    }

    public Set<TopicPartition> authorizedMatchingTopicPartitions() {
        final Pattern pattern = patternFromQuery.pattern();
        LOGGER.info("Filtering topics using pattern <{}>", pattern.pattern());

        // get all authorized topics in the kafka cluster and filter using regex
        final Set<String> matchingTopics = consumer
                .listTopics()
                .keySet()
                .stream()
                .filter(topic -> pattern.matcher(topic).matches())
                .collect(Collectors.toSet());

        final Set<TopicPartition> authorizedTopicPartitions = new HashSet<>();

        // check that authorized to all topics
        for (final String topic : matchingTopics) {
            try {
                final List<PartitionInfo> partitions = consumer.partitionsFor(topic); // can throw TopicAuthorizationException same as poll()
                authorizedTopicPartitions
                        .addAll(
                                partitions
                                        .stream()
                                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                                        .collect(Collectors.toSet())
                        );
            }
            catch (final TopicAuthorizationException e) {
                LOGGER.info("Skipping unauthorized topic <{}>", topic);
            }
        }

        if (authorizedTopicPartitions.isEmpty()) {
            LOGGER.warn("Found no authorized topics found for pattern <{}>, no kafka data will be available", pattern);
        }

        return authorizedTopicPartitions;
    }

    @Override
    public Map<TopicPartition, Long> getInitialEndOffsets() {
        if (persistedEndOffsetMap.isEmpty() || isContinuouslyProcessing) {
            final Set<TopicPartition> topicPartitions = authorizedMatchingTopicPartitions();
            final Map<TopicPartition, Long> topicPartitionEndOffsetMap = new HashMap<>();
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions, Duration.ofSeconds(60));

            for (final TopicPartition topicPartition : topicPartitions) {
                final long partitionEnd = endOffsets.get(topicPartition);
                topicPartitionEndOffsetMap.put(topicPartition, partitionEnd);

            }
            persistedEndOffsetMap.putAll(topicPartitionEndOffsetMap);
            return topicPartitionEndOffsetMap;
        }
        else {
            return persistedEndOffsetMap;
        }
    }

    @Override
    public Map<TopicPartition, Long> getEndOffsets(final KafkaOffset startOffset) {
        return getInitialEndOffsets();
    }

    @Override
    public Map<TopicPartition, Long> getBeginningOffsets(final KafkaOffset endOffset) {
        final Set<TopicPartition> topicPartitions = authorizedMatchingTopicPartitions();
        final Map<TopicPartition, Long> topicPartitionStartOffsetMap = new HashMap<>();
        final Map<TopicPartition, Long> beginningOffsets = consumer
                .beginningOffsets(topicPartitions, Duration.ofSeconds(60));

        for (final TopicPartition topicPartition : topicPartitions) {
            final long partitionStart = beginningOffsets.get(topicPartition); // partition start becomes the current start
            topicPartitionStartOffsetMap.put(topicPartition, partitionStart);
        }
        return topicPartitionStartOffsetMap;
    }

    @Override
    public void commit(KafkaOffset offset) {
    }
}
