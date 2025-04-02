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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaQueryImplTest {

    private final Set<TopicPartition> authorizedTopics = new HashSet<>();
    private final Set<TopicPartition> unauthorizedTopics = new HashSet<>();

    @BeforeAll
    public void setup() {
        authorizedTopics.add(new TopicPartition("auth-topic-1", 0));
        authorizedTopics.add(new TopicPartition("auth-topic-2", 0));
        unauthorizedTopics.add(new TopicPartition("unauth-topic-1", 0));
        unauthorizedTopics.add(new TopicPartition("unauth-topic-2", 0));
    }

    @Test
    public void testMatchAnyTopic() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        final KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                new KafkaSubscriptionPatternFromQuery("<index value=\"*\" operation=\"EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(2, topicPartitions.size());
        final List<String> topicNames = topicPartitions
                .stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toList());
        Assertions.assertTrue(topicNames.contains("auth-topic-1"));
        Assertions.assertTrue(topicNames.contains("auth-topic-2"));
    }

    @Test
    public void testNullTopicFromWalker() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                // operation NOT_EQUALS causes KafkaWalker to return a null String, this is replaced to match all regex
                new KafkaSubscriptionPatternFromQuery("<index value=\"unauth-topic-1\" operation=\"NOT_EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(2, topicPartitions.size());
        final List<String> topicNames = topicPartitions
                .stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toList());
        Assertions.assertTrue(topicNames.contains("auth-topic-1"));
        Assertions.assertTrue(topicNames.contains("auth-topic-2"));
    }

    @Test
    public void testAuthorizedMatchingTopic() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        final KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                new KafkaSubscriptionPatternFromQuery("<index value=\"auth-topic-1\" operation=\"EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(1, topicPartitions.size());
        Assertions.assertEquals("auth-topic-1", topicPartitions.stream().findFirst().get().topic());
    }

    @Test
    public void testAuthorizedMatchingTopicWildcard() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        final KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                new KafkaSubscriptionPatternFromQuery("<index value=\"auth-topic-*\" operation=\"EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(2, topicPartitions.size());
        final List<String> topicNames = topicPartitions
                .stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toList());
        Assertions.assertTrue(topicNames.contains("auth-topic-1"));
        Assertions.assertTrue(topicNames.contains("auth-topic-2"));
    }

    @Test
    public void testAuthorizedMatchingTopicDoesNotGetUnauthorized() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        final KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                new KafkaSubscriptionPatternFromQuery("<index value=\"unauth-topic-1\" operation=\"EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(0, topicPartitions.size());
    }

    @Test
    public void testAuthorizedMatchingTopicDoesNotGetUnauthorizedWildcard() {
        final Consumer<byte[], byte[]> consumer = mockConsumer();
        final KafkaQueryImpl kafkaQuery = new KafkaQueryImpl(
                new KafkaSubscriptionPatternFromQuery("<index value=\"unauth-topic-*\" operation=\"EQUALS\"/>"),
                consumer,
                new HashMap<>(),
                false
        );
        final Set<TopicPartition> topicPartitions = kafkaQuery.authorizedMatchingTopicPartitions();
        Assertions.assertEquals(0, topicPartitions.size());
    }

    private Consumer<byte[], byte[]> mockConsumer() {
        final Consumer<byte[], byte[]> consumerMock = mock(KafkaConsumer.class);
        final Map<String, List<PartitionInfo>> allTopics = new HashMap<>();
        for (final TopicPartition partition : authorizedTopics) {
            allTopics
                    .put(
                            partition.topic(),
                            Collections.singletonList(new PartitionInfo(partition.topic(), 0, null, null, null))
                    );
        }

        for (final TopicPartition partition : unauthorizedTopics) {
            allTopics
                    .put(
                            partition.topic(),
                            Collections.singletonList(new PartitionInfo(partition.topic(), 0, null, null, null))
                    );
        }

        when(consumerMock.listTopics()).thenReturn(allTopics);

        for (final TopicPartition partition : unauthorizedTopics) {
            when(consumerMock.partitionsFor(partition.topic()))
                    .thenThrow(new TopicAuthorizationException(String.valueOf(Collections.singletonList(partition))));
        }

        for (final TopicPartition partition : authorizedTopics) {
            when(consumerMock.partitionsFor(partition.topic()))
                    .thenReturn(Collections.singletonList(new PartitionInfo(partition.topic(), 0, null, null, null)));
        }

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();

        for (final TopicPartition partition : authorizedTopics) {
            beginningOffsets.put(partition, 10L);
            endOffsets.put(partition, 100L);
        }

        final Duration duration = Duration.ofSeconds(60);
        when(consumerMock.beginningOffsets(authorizedTopics, duration)).thenReturn(beginningOffsets);
        when(consumerMock.endOffsets(authorizedTopics, duration)).thenReturn(endOffsets);

        return consumerMock;
    }

}
