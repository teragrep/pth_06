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

public class KafkaQueryImpl implements KafkaQuery {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueryImpl.class);

    private final KafkaSubscriptionPatternFromQuery patternFromQuery;
    private final Consumer<byte[], byte[]> consumer;
    private final Map<TopicPartition, Long> persistedEndOffsetMap;
    private final boolean isContinuouslyProcessing;

    public KafkaQueryImpl(final Config config) {
        this(new KafkaSubscriptionPatternFromQuery(config.query), new KafkaConsumer<>(config.kafkaConfig.driverOpts), new HashMap<>(), config.kafkaConfig.kafkaContinuousProcessing);
    }

    public KafkaQueryImpl(final KafkaSubscriptionPatternFromQuery patternFromQuery, final Consumer<byte[], byte[]> consumer, final Map<TopicPartition, Long> persistedEndOffsetMap, final boolean isContinuouslyProcessing) {
        this.patternFromQuery = patternFromQuery;
        this.consumer = consumer;
        this.persistedEndOffsetMap = persistedEndOffsetMap;
        this.isContinuouslyProcessing = isContinuouslyProcessing;
    }

    public Set<TopicPartition> authorizedMatchingTopicPartitions() {
        final Pattern pattern = patternFromQuery.pattern();
        final Set<String> matchingTopics = consumer.listTopics().keySet().stream()
                .filter(topic -> pattern.matcher(topic).matches())
                .collect(Collectors.toSet());

        final Set<TopicPartition> authorizedTopicPartitions = new HashSet<>();

        // filter out topics that do not match regex or that have no authorization
        for (final String topic: matchingTopics) {
            try {
                final List<PartitionInfo> partitions = consumer.partitionsFor(topic); // can throw TopicAuthorizationException
                authorizedTopicPartitions.addAll(partitions.stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition())).collect(Collectors.toSet()
                        )
                );
            } catch (final TopicAuthorizationException e) {
                LOGGER.info("Skipping unauthorized topic <{}>", topic);
            }
        }

        if (authorizedTopicPartitions.isEmpty()) {
            LOGGER.warn("Found no authorized topics for given pattern <{}>", pattern);
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
