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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaQueryImpl implements KafkaQuery {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueryImpl.class);

    private final String query;
    private final Consumer<byte[], byte[]> consumer;
    private final Map<TopicPartition, Long> persistedEndOffsetMap;
    private final boolean isContinuouslyProcessing;

    public KafkaQueryImpl(final Config config) {
        this(config.query, new KafkaConsumer<>(config.kafkaConfig.driverOpts), new HashMap<>(), config.kafkaConfig.kafkaContinuousProcessing);
    }

    public KafkaQueryImpl(final String query, final Consumer<byte[], byte[]> consumer, final Map<TopicPartition, Long> persistedEndOffsetMap, final boolean isContinuouslyProcessing) {
        this.query = query;
        this.consumer = consumer;
        this.persistedEndOffsetMap = persistedEndOffsetMap;
        this.isContinuouslyProcessing = isContinuouslyProcessing;
    }

    @Override
    public Map<TopicPartition, Long> getInitialEndOffsets() {
        return getEndOffsets(null);
    }

    @Override
    public Map<TopicPartition, Long> getEndOffsets(final KafkaOffset startOffset) {
        final Set<TopicPartition> topicPartitionSet = topicPartitionSet();

        if (persistedEndOffsetMap.isEmpty() || isContinuouslyProcessing) {
            final Map<TopicPartition, Long> topicPartitionEndOffsetMap = new HashMap<>();
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitionSet, Duration.ofSeconds(60)); // TODO parametrize

            for (final TopicPartition topicPartition : topicPartitionSet) {
                final long partitionEnd = endOffsets.get(topicPartition); // partition end becomes the current end

                topicPartitionEndOffsetMap.put(topicPartition, partitionEnd);
            }
            persistedEndOffsetMap.putAll(topicPartitionEndOffsetMap);
            return topicPartitionEndOffsetMap;
        } else {
            return persistedEndOffsetMap;
        }
    }

    @Override
    public Map<TopicPartition, Long> getBeginningOffsets(final KafkaOffset endOffset) {
        final Set<TopicPartition> topicPartitionSet = topicPartitionSet();
        final Map<TopicPartition, Long> topicPartitionStartOffsetMap = new HashMap<>();

        final Map<TopicPartition, Long> beginningOffsets = consumer
                .beginningOffsets(topicPartitionSet, Duration.ofSeconds(60));

        for (final TopicPartition topicPartition : topicPartitionSet) {
            final long partitionStart = beginningOffsets.get(topicPartition); // partition start becomes the current start
            topicPartitionStartOffsetMap.put(topicPartition, partitionStart);
        }
        return topicPartitionStartOffsetMap;
    }

    @Override
    public void commit(KafkaOffset offset) {

    }

    private Set<TopicPartition> topicPartitionSet() {
        final String regexString = new RegexStringFromQuery(query).regexString();
        final Pattern topicsRegex = Pattern.compile(regexString);

        Set<TopicPartition> localTopicPartitionSet = Collections.emptySet();
        final int maxRetries = 5;
        int retries = 0;

        while (localTopicPartitionSet.isEmpty() && (retries < maxRetries)) {
            final Map<String, List<PartitionInfo>> listTopics;
            try {
                listTopics = consumer.listTopics(Duration.ofSeconds(60));
            } catch (final TopicAuthorizationException topicAuthorizationException) {
                handleTopicAuthorizationException(regexString, topicAuthorizationException);
                continue;
            }

            boolean hasTopicMatch = false;

            for (final Map.Entry<String, List<PartitionInfo>> entry : listTopics.entrySet()) {
                final Matcher matcher = topicsRegex.matcher(entry.getKey());
                if (matcher.matches()) {
                    hasTopicMatch = true;

                    for (PartitionInfo partitionInfo : entry.getValue()) {
                        TopicPartition topicPartition = new TopicPartition(entry.getKey(), partitionInfo.partition());
                        consumer.assign(Collections.singleton(topicPartition));
                    }
                }
            }

            // If no topic matches the pattern, warn and break
            if (!hasTopicMatch && !listTopics.isEmpty()) {
                LOGGER.warn("Subscribed to topics that do not exist! Kafka data will not be available.");
                break;
            }

            localTopicPartitionSet = consumer.assignment();

            if (localTopicPartitionSet.isEmpty()) {
                retries++;
                LOGGER.warn("Kafka consumer assignment returned no topics, retry count <{}>", retries);
            }
        }
        return localTopicPartitionSet;
    }

    private void handleTopicAuthorizationException(
            final String topicsRegexString,
            final TopicAuthorizationException topicAuthorizationException
    ) {
        // wildcard queries may match topics with no authorization
        final Set<String> unauthorizedTopics = topicAuthorizationException.unauthorizedTopics();

        // use negative lookahead to cancel unauthorizedTopics:
        // by prepending it to authorized topics
        // i.e (?!^not_a|not_b|not_c$)^(yes_x|(yes_y|yes_z))$
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(?!");

        int topicsProcessed = 0;
        for (final String topic : unauthorizedTopics) {
            stringBuilder.append("^");
            stringBuilder.append(topic);
            stringBuilder.append("$");
            topicsProcessed++;

            if (topicsProcessed < unauthorizedTopics.size()) {
                stringBuilder.append("|");
            }
        }
        stringBuilder.append(")");

        stringBuilder.append(topicsRegexString);

        final String negatedTopicRegexString = stringBuilder.toString();

        final Pattern negatedTopicRegexPattern = Pattern.compile(negatedTopicRegexString);

        LOGGER.warn(
                "Re-subscribing: <[{}]> after TopicAuthorizationException: <{}>",
                negatedTopicRegexPattern, topicAuthorizationException
        );

        consumer.subscribe(negatedTopicRegexPattern);

        // it seems that kafka may not report all unauthorized topics in a
        // one go, therefore recursion is needed
        try {
            consumer.listTopics(Duration.ofSeconds(60));
        } catch (final TopicAuthorizationException authorizationException) {
            handleTopicAuthorizationException(negatedTopicRegexString, authorizationException);
        }
    }
}
