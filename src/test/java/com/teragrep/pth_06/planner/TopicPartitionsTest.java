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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class TopicPartitionsTest {

    @Test
    public void testAuthorizedTopics() {
        final Consumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        final Map<String, List<PartitionInfo>> topics = new HashMap<>();
        final List<PartitionInfo> partitionInfos = Collections
                .singletonList(new PartitionInfo("topic-1", 0, null, null, null));
        topics.put("topic-1", partitionInfos);
        when(consumer.partitionsFor("topic-1")).thenReturn(partitionInfos);
        when(consumer.listTopics(Duration.ofSeconds(60))).thenReturn(topics);
        final TopicPartitions topicPartitions = new TopicPartitions("topic-1", consumer);
        final List<TopicPartition> partitions = topicPartitions.asList();
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("topic-1", partitions.get(0).topic());
        Assertions.assertEquals(0, partitions.get(0).partition());
    }

    @Test
    public void testSkipUnauthorizedTopic() {
        final Consumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        final Map<String, List<PartitionInfo>> initialTopics = new HashMap<>();
        final List<PartitionInfo> firstTopicInfo = Collections
                .singletonList(new PartitionInfo("topic-1", 0, null, null, null));
        initialTopics.put("topic-1", firstTopicInfo);
        when(consumer.partitionsFor("topic-1")).thenThrow(TopicAuthorizationException.class);
        when(consumer.listTopics(Duration.ofSeconds(60))).thenReturn(initialTopics);
        final TopicPartitions topicPartitions = new TopicPartitions("topic-1", consumer);
        final List<TopicPartition> partitions = topicPartitions.asList();
        Assertions.assertEquals(0, partitions.size());
    }
}
