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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class KafkaQueryImplTest {

    private final KafkaContainer kafka = Assertions
            .assertDoesNotThrow(() -> new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0")));
    Map<String, String> opts;
    AdminClient adminClient;
    KafkaProducer<String, String> producer;

    @BeforeEach
    public void start() {
        Assertions.assertDoesNotThrow(kafka::start);
        this.opts = new HashMap<>();
        opts.put("queryXML", "<index value=\"*\" operation=\"EQUALS\"/>");
        opts.put("kafka.enabled", "true");
        opts.put("kafka.bootstrap.servers", kafka.getBootstrapServers());
        opts.put("kafka.sasl.mechanism", "GSSAPI");
        opts.put("kafka.security.protocol", "PLAINTEXT");
        opts.put("kafka.sasl.jaas.config", "");
        this.adminClient = AdminClient
                .create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        final Map<String, Object> producerOpts = new HashMap<>();
        producerOpts.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerOpts.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerOpts.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(producerOpts);
    }

    @AfterEach
    public void tearDown() {
        Assertions.assertDoesNotThrow(() -> adminClient.close(10, TimeUnit.SECONDS));
        Assertions.assertDoesNotThrow(() -> producer.close(10, TimeUnit.SECONDS));
        Assertions.assertDoesNotThrow(kafka::stop);
    }

    @Test
    @EnabledIfSystemProperty(
            named = "runContainerTests",
            matches = "true"
    )
    public void testOffsetWithoutTopics() {
        final KafkaQuery kafkaQuery = new KafkaQueryImpl(new Config(opts));
        final long start = System.nanoTime();
        final Collection<Long> beginningOffsetsMap = kafkaQuery.beginningOffsets().values();
        Assertions.assertEquals(0, beginningOffsetsMap.size());
        final Collection<Long> endOffsetsMap = kafkaQuery.endOffsets().values();
        Assertions.assertEquals(0, endOffsetsMap.size());
        final long end = System.nanoTime();
        final long executionTime = end - start;
        Assertions.assertTrue(executionTime < 10L * 1E9, "should not wait for topics to appear in kafka");
    }

    @Test
    @EnabledIfSystemProperty(
            named = "runContainerTests",
            matches = "true"
    )
    public void testOffsetsWithoutRecords() {
        final String emptyTopic = "empty-topic-1";
        final NewTopic topic = new NewTopic(emptyTopic, 1, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Collections.singleton(topic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final boolean topicCreated = Assertions
                .assertDoesNotThrow(() -> adminClient.listTopics().names().get(10, TimeUnit.SECONDS).contains(emptyTopic));
        Assertions.assertTrue(topicCreated);
        final KafkaQuery kafkaQuery = new KafkaQueryImpl(new Config(opts), Pattern.compile("^empty-topic-1$"));
        final long start = System.nanoTime();
        final Collection<Long> beginningOffsets = kafkaQuery.beginningOffsets().values();
        final Collection<Long> endOffsets = kafkaQuery.endOffsets().values();
        Assertions.assertEquals(1, beginningOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : beginningOffsets) {
            Assertions.assertEquals(0, offset, "partition should not have any records");
        }
        Assertions.assertEquals(1, endOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : endOffsets) {
            Assertions.assertEquals(0, offset, "partition should not have any records");
        }
        final long end = System.nanoTime();
        final long kafkaQueryExecutiontime = end - start;
        Assertions
                .assertTrue(
                        kafkaQueryExecutiontime < 3L * 1E9,
                        "topics without records should not wait for records to appear in kafka"
                );
    }

    @Test
    @EnabledIfSystemProperty(
            named = "runContainerTests",
            matches = "true"
    )
    public void testOffsetWithRecords() {
        final String topicName = "topic-1";
        final NewTopic topic = new NewTopic(topicName, 1, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Collections.singleton(topic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final boolean topicCreated = Assertions
                .assertDoesNotThrow(() -> adminClient.listTopics().names().get().contains(topicName));
        Assertions.assertTrue(topicCreated);
        final ProducerRecord<String, String> record1 = new ProducerRecord<>(topicName, "key-1", "message-1");
        final ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, "key-2", "message-2");
        producer.send(record1, (meta, exception) -> Assertions.assertNull(exception));
        producer.send(record2, (meta, exception) -> Assertions.assertNull(exception));
        final KafkaQuery kafkaQuery = new KafkaQueryImpl(new Config(opts), Pattern.compile("^topic-1$"));
        final Collection<Long> beginningOffsets = kafkaQuery.beginningOffsets().values();
        final Collection<Long> endOffsets = kafkaQuery.endOffsets().values();
        Assertions.assertEquals(1, beginningOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : beginningOffsets) {
            Assertions.assertEquals(0, offset, "beginning offset should be at 0");
        }
        Assertions.assertEquals(1, endOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : endOffsets) {
            Assertions.assertEquals(2, offset, "end offset should be at 2 since end offset is exclusive");
        }
    }

    @Test
    @EnabledIfSystemProperty(
            named = "runContainerTests",
            matches = "true"
    )
    public void testImmutabilityWhenNotContinuouslyProcessing() {
        final String topicName = "topic-1";
        final NewTopic topic = new NewTopic(topicName, 1, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Collections.singleton(topic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final boolean topicCreated = Assertions
                .assertDoesNotThrow(() -> adminClient.listTopics().names().get().contains(topicName));
        Assertions.assertTrue(topicCreated);
        final ProducerRecord<String, String> record1 = new ProducerRecord<>(topicName, "key-1", "message-1");
        final ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, "key-2", "message-2");
        producer.send(record1, (meta, exception) -> Assertions.assertNull(exception));
        producer.send(record2, (meta, exception) -> Assertions.assertNull(exception));
        final KafkaQuery kafkaQuery = new KafkaQueryImpl(new Config(opts), Pattern.compile("^topic-1$"));
        final Collection<Long> beginningOffsets = kafkaQuery.beginningOffsets().values();
        final Collection<Long> endOffsets = kafkaQuery.endOffsets().values();
        Assertions.assertEquals(1, beginningOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : beginningOffsets) {
            Assertions.assertEquals(0, offset, "beginning offset should be at 0");
        }
        Assertions.assertEquals(1, endOffsets.size(), "topic should have exactly one partition");
        for (final Long offset : endOffsets) {
            Assertions.assertEquals(2, offset, "end offset should be at 2 since end offset is exclusive");
        }
        // add more records
        final ProducerRecord<String, String> record3 = new ProducerRecord<>(topicName, "key-3", "message-3");
        final ProducerRecord<String, String> record4 = new ProducerRecord<>(topicName, "key-4", "message-4");
        producer.send(record3, (meta, exception) -> Assertions.assertNull(exception));
        producer.send(record4, (meta, exception) -> Assertions.assertNull(exception));
        // offsets should not be changed
        Assertions.assertEquals(beginningOffsets.size(), kafkaQuery.beginningOffsets().size());
        Assertions.assertEquals(endOffsets.size(), kafkaQuery.endOffsets().size());
    }

    @Test
    @EnabledIfSystemProperty(
            named = "runContainerTests",
            matches = "true"
    )
    public void testContinuousProcessing() {
        final Map<String, String> withContinuousProcessing = new HashMap<>(opts);
        withContinuousProcessing.put("kafka.continuousProcessing", "true");
        final String topicName = "topic-1";
        final NewTopic topic = new NewTopic(topicName, 1, (short) 1);
        final KafkaFuture<Void> all = adminClient.createTopics(Collections.singleton(topic)).all();
        Assertions.assertDoesNotThrow(() -> all.get(10, TimeUnit.SECONDS));
        final boolean topicCreated = Assertions
                .assertDoesNotThrow(() -> adminClient.listTopics().names().get().contains(topicName));
        Assertions.assertTrue(topicCreated);
        final ProducerRecord<String, String> record1 = new ProducerRecord<>(topicName, "key-1", "message-1");
        final ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, "key-2", "message-2");
        producer.send(record1, (meta, exception) -> Assertions.assertNull(exception));
        producer.send(record2, (meta, exception) -> Assertions.assertNull(exception));
        final KafkaQuery kafkaQuery = new KafkaQueryImpl(
                new Config(withContinuousProcessing),
                Pattern.compile("^topic-1$")
        );
        final Collection<Long> beginningOffsets = kafkaQuery.beginningOffsets().values();
        final Collection<Long> endOffsets = kafkaQuery.endOffsets().values();
        Assertions.assertEquals(1, beginningOffsets.size(), "topic should have exactly one partition");
        int beginningLoops = 0;
        for (final Long offset : beginningOffsets) {
            beginningLoops++;
            Assertions.assertEquals(0, offset, "beginning offset should be at 0");
        }
        Assertions.assertEquals(1, beginningLoops);
        Assertions.assertEquals(1, endOffsets.size(), "topic should have exactly one partition");
        int endLoops = 0;
        for (final Long offset : endOffsets) {
            endLoops++;
            Assertions.assertEquals(2, offset, "end offset should be at 2 since end offset is exclusive");
        }
        Assertions.assertEquals(1, endLoops);
        // add more records
        final ProducerRecord<String, String> record3 = new ProducerRecord<>(topicName, "key-3", "message-3");
        final ProducerRecord<String, String> record4 = new ProducerRecord<>(topicName, "key-4", "message-4");
        producer.send(record3, (meta, exception) -> Assertions.assertNull(exception));
        producer.send(record4, (meta, exception) -> Assertions.assertNull(exception));
        // end offset should be moved
        final Collection<Long> updatedEndOffsets = kafkaQuery.endOffsets().values();
        int updatedLoops = 0;
        for (final Long offset : updatedEndOffsets) {
            updatedLoops++;
            Assertions.assertEquals(4, offset, "end offset should be at 4 since end offset is exclusive");
        }
        Assertions.assertEquals(1, updatedLoops);
    }
}
