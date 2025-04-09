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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(
        named = "runContainerTests",
        matches = "true"
)
public final class CombinedTopicPartitionsTest {

    private final static KafkaContainer kafka = Assertions
            .assertDoesNotThrow(() -> new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0")));
    AdminClient adminClient;
    KafkaProducer<String, String> producer;
    KafkaConsumer<byte[], byte[]> consumer;

    @BeforeEach
    public void start() {
        Assertions.assertDoesNotThrow(kafka::start);
        this.adminClient = AdminClient
                .create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        final Map<String, Object> producerOpts = new HashMap<>();
        producerOpts.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerOpts.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerOpts.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(producerOpts);
        final Map<String, Object> consumerOpts = new HashMap<>();
        consumerOpts.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerOpts.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        consumerOpts.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerOpts.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(consumerOpts);
    }

    @AfterEach
    public void tearDown() {
        Assertions.assertDoesNotThrow(() -> adminClient.close(10, TimeUnit.SECONDS));
        Assertions.assertDoesNotThrow(() -> producer.close(10, TimeUnit.SECONDS));
        Assertions.assertDoesNotThrow(() -> consumer.close());
        Assertions.assertDoesNotThrow(kafka::stop);
    }

    @Test
    public void testCombinedPartitions() {
        final String firstTopicName = "topic-1";
        final String secondTopicName = "topic-2";
        final NewTopic firstTopic = new NewTopic(firstTopicName, 3, (short) 1);
        final NewTopic secondTopic = new NewTopic(secondTopicName, 2, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Arrays.asList(firstTopic, secondTopic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final Set<String> topicsInKafka = assertDoesNotThrow(() -> adminClient.listTopics().names().get());
        Assertions.assertTrue(topicsInKafka.contains(firstTopicName));
        Assertions.assertTrue(topicsInKafka.contains(secondTopicName));
        final CombinedTopicPartitions combinedTopicPartitions = new CombinedTopicPartitions(
                new TopicsFromKafka(consumer),
                consumer
        );
        Assertions.assertEquals(5, combinedTopicPartitions.asList().size());
    }

    @Test
    public void testNoTopics() {
        final Set<String> topicsInKafka = assertDoesNotThrow(() -> adminClient.listTopics().names().get());
        Assertions.assertTrue(topicsInKafka.isEmpty());
        final CombinedTopicPartitions combinedTopicPartitions = new CombinedTopicPartitions(
                new TopicsFromKafka(consumer),
                consumer
        );
        Assertions.assertEquals(0, combinedTopicPartitions.asList().size());
    }

    @Test
    public void testImmutability() {
        final Set<String> topicsInKafka = assertDoesNotThrow(() -> adminClient.listTopics().names().get());
        Assertions.assertTrue(topicsInKafka.isEmpty());
        final CombinedTopicPartitions combinedTopicPartitions = new CombinedTopicPartitions(
                new TopicsFromKafka(consumer),
                consumer
        );
        Assertions.assertEquals(0, combinedTopicPartitions.asList().size());
        final String firstTopicName = "topic-1";
        final String secondTopicName = "topic-2";
        final NewTopic firstTopic = new NewTopic(firstTopicName, 3, (short) 1);
        final NewTopic secondTopic = new NewTopic(secondTopicName, 2, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Arrays.asList(firstTopic, secondTopic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final Set<String> topicsAfterCreation = assertDoesNotThrow(() -> adminClient.listTopics().names().get());
        Assertions.assertEquals(2, topicsAfterCreation.size());
        Assertions.assertEquals(0, combinedTopicPartitions.asList().size());
    }
}
