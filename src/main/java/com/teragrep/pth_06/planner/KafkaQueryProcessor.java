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

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.pth_06.config.Config;
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import com.teragrep.pth_06.planner.walker.KafkaWalker;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>Kafka Query Processor</h1> Class for processing kafka queries.
 *
 * @see KafkaQuery
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
public class KafkaQueryProcessor implements KafkaQuery {

    // NOTE, for Kafka: start is inclusive (GreaterOrEqualThan), end is exclusive
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueryProcessor.class);

    private final boolean continuousProcessing;
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final Set<TopicPartition> topicPartitionSet;
    private final Map<TopicPartition, Long> persistedEndOffsetMap;

    public KafkaQueryProcessor(Config config) {
        this.persistedEndOffsetMap = new HashMap<>();
        this.continuousProcessing = config.kafkaConfig.kafkaContinuousProcessing;
        Map<String, Object> executorKafkaProperties = config.kafkaConfig.driverOpts;
        this.kafkaConsumer = new KafkaConsumer<>(executorKafkaProperties);

        String topicsRegexString = null;
        if (config.query != null) {
            try {
                KafkaWalker parser = new KafkaWalker();
                topicsRegexString = parser.fromString(config.query);
            }
            catch (ParserConfigurationException | IOException | SAXException ex) {
                ex.printStackTrace();
                throw new RuntimeException(
                        "KafkaQueryProcessor problems when construction Query conditions query:" + config.query
                                + " exception:" + ex
                );
            }
        }

        if (topicsRegexString == null) {
            topicsRegexString = "^.*$"; // all topics if none given
        }

        Pattern topicsRegex = Pattern.compile(topicsRegexString);
        LOGGER.info("Subscribing to: " + topicsRegexString);
        kafkaConsumer.subscribe(topicsRegex);

        Set<TopicPartition> localTopicPartitionSet = Collections.emptySet();
        while (localTopicPartitionSet.isEmpty()) {
            try {
                kafkaConsumer.poll(Duration.ofSeconds(60));
            }
            catch (TopicAuthorizationException topicAuthorizationException) {
                handleTopicAuthorizationException(topicsRegexString, topicAuthorizationException);
            }
            localTopicPartitionSet = kafkaConsumer.assignment();

            if (localTopicPartitionSet.isEmpty()) {
                LOGGER.warn("kafkaConsumer.assignment() returned no topics, verifying subscription and retrying");

                // check if connectivity issue or if illegal pattern
                Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics(Duration.ofSeconds(60));
                boolean hasTopicMatch = false;
                for (Map.Entry<String, List<PartitionInfo>> entry : listTopics.entrySet()) {
                    Matcher matcher = topicsRegex.matcher(entry.getKey());
                    if (matcher.matches()) {
                        hasTopicMatch = true;
                    }
                }

                if (!hasTopicMatch && !listTopics.isEmpty()) {
                    //throw new IllegalStateException("subscribed to non-existing topic");
                    // passthrough
                    localTopicPartitionSet = Collections.emptySet();
                    LOGGER.warn("subscribed to topics that do not exist! Kafka data will not be available.");
                    break;
                }

            }
        }
        topicPartitionSet = localTopicPartitionSet;
        LOGGER.debug("KafkaQueryProcessor");
    }

    private void handleTopicAuthorizationException(
            String topicsRegexString,
            TopicAuthorizationException topicAuthorizationException
    ) {
        // wildcard queries may match topics with no authorization
        Set<String> unauthorizedTopics = topicAuthorizationException.unauthorizedTopics();

        // use negative lookahead to cancel unauthorizedTopics:
        // by prepending it to authorized topics
        // i.e (?!^not_a|not_b|not_c$)^(yes_x|(yes_y|yes_z))$
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(?!");

        int topicsProcessed = 0;
        for (String topic : unauthorizedTopics) {
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

        String negatedTopicRegexString = stringBuilder.toString();

        Pattern negatedTopicRegexPattern = Pattern.compile(negatedTopicRegexString);

        LOGGER
                .warn(
                        "Re-subscribing: <[" + negatedTopicRegexPattern + "]>" + " after TopicAuthorizationException: "
                                + topicAuthorizationException
                );
        kafkaConsumer.subscribe(negatedTopicRegexPattern);

        // it seems that kafka may not report all unauthorized topics in a
        // one go, therefore recursion is needed
        try {
            kafkaConsumer.poll(Duration.ofSeconds(60));
        }
        catch (TopicAuthorizationException authorizationException) {
            handleTopicAuthorizationException(negatedTopicRegexString, authorizationException);
        }
    }

    @VisibleForTesting
    public KafkaQueryProcessor(Consumer<byte[], byte[]> consumer) {
        this.persistedEndOffsetMap = new HashMap<>();
        this.continuousProcessing = false;
        kafkaConsumer = consumer;
        topicPartitionSet = consumer.assignment();
        LOGGER.debug("@VisibleForTesting KafkaQueryProcessor");
    }

    @Override
    public Map<TopicPartition, Long> getInitialEndOffsets() {
        if (persistedEndOffsetMap.isEmpty() || continuousProcessing) {

            Map<TopicPartition, Long> topicPartitionEndOffsetMap = new HashMap<>();
            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitionSet, Duration.ofSeconds(60));

            for (TopicPartition topicPartition : topicPartitionSet) {
                long partitionEnd = endOffsets.get(topicPartition);
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
    public Map<TopicPartition, Long> getEndOffsets(KafkaOffset startOffset) {
        if (persistedEndOffsetMap.isEmpty() || continuousProcessing) {
            Map<TopicPartition, Long> topicPartitionEndOffsetMap = new HashMap<>();

            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitionSet, Duration.ofSeconds(60)); // TODO parametrize

            for (TopicPartition topicPartition : topicPartitionSet) {
                long partitionEnd = endOffsets.get(topicPartition); // partition end becomes the current end

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
    public Map<TopicPartition, Long> getBeginningOffsets(KafkaOffset endOffset) {
        Map<TopicPartition, Long> topicPartitionStartOffsetMap = new HashMap<>();

        Map<TopicPartition, Long> beginningOffsets = kafkaConsumer
                .beginningOffsets(topicPartitionSet, Duration.ofSeconds(60));

        for (TopicPartition topicPartition : topicPartitionSet) {
            long partitionStart = beginningOffsets.get(topicPartition); // partition start becomes the current start
            topicPartitionStartOffsetMap.put(topicPartition, partitionStart);
        }
        return topicPartitionStartOffsetMap;
    }

    @Override
    public void commit(KafkaOffset offset) {
        // no-op
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
