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
package com.teragrep.pth_06.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class KafkaConfig {

    public final String consumerGroupId;
    public final Map<String, Object> driverOpts;
    public final Map<String, Object> executorOpts;

    public final Map<String, String> executorConfig;

    public final boolean kafkaContinuousProcessing;

    public final boolean skipNonRFC5424Records;
    public final boolean isStub;

    public KafkaConfig(Map<String, String> opts) {
        consumerGroupId = UUID.randomUUID().toString();
        driverOpts = parseKafkaDriverOpts(opts);
        executorOpts = parseKafkaExecutorOpts(opts);
        executorConfig = parseKafkaExecutorConfig(opts);
        kafkaContinuousProcessing = opts.getOrDefault("kafka.continuousProcessing", "false").equalsIgnoreCase("true");

        // TODO: Rename to match kafka config name
        skipNonRFC5424Records = opts.getOrDefault("skipNonRFC5424Files", "false").equalsIgnoreCase("true");

        isStub = false;
    }

    public KafkaConfig() {
        consumerGroupId = "";
        driverOpts = new HashMap<>();
        executorOpts = new HashMap<>();
        executorConfig = new HashMap<>();
        kafkaContinuousProcessing = false;
        skipNonRFC5424Records = false;

        isStub = true;
    }

    private Map<String, Object> parseKafkaCommonOpts(Map<String, String> opts) {
        // options common for driver and executor
        Map<String, Object> rv = new HashMap<>();
        String bootstrapServers = opts.get("kafka.bootstrap.servers");

        if (bootstrapServers == null) {
            throw new IllegalArgumentException("missing kafka.bootstrap.servers");
        }
        rv.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String saslMechanism = opts.get("kafka.sasl.mechanism");
        if (saslMechanism == null) {
            throw new IllegalArgumentException("missing kafka.sasl.mechanism");
        }
        rv.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

        String securityProtocol = opts.get("kafka.security.protocol");
        if (securityProtocol == null) {
            throw new IllegalArgumentException("missing kafka.security.protocol");
        }
        rv.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        String jaasConfig = opts.get("kafka.sasl.jaas.config");
        if (jaasConfig == null) {
            throw new IllegalArgumentException("missing kafka.sasl.jaas.config");
        }
        rv.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        // hardcoded values
        rv
                .put(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                );
        rv
                .put(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                );
        rv.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        rv.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536);
        rv.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 86400000); // avoid interrupt

        return rv;
    }

    private Map<String, Object> parseKafkaDriverOpts(Map<String, String> opts) {
        // driver specific options
        Map<String, Object> rv = parseKafkaCommonOpts(opts);
        rv.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "-driver");
        rv.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        rv.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // driver should not

        return rv;
    }

    private Map<String, Object> parseKafkaExecutorOpts(Map<String, String> opts) {
        // executor specific options
        Map<String, Object> rv = parseKafkaCommonOpts(opts);

        rv.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "-executor");
        rv.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String maxPollRecords = opts.getOrDefault("kafka.max.poll.records", "1000");
        rv.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(maxPollRecords));

        String fetchMaxBytes = opts.getOrDefault("kafka.fetch.max.bytes", "1024");
        rv.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.parseInt(fetchMaxBytes));

        String fetchMaxWait = opts.getOrDefault("kafka.fetch.max.wait.ms", String.valueOf(120 * 1000));
        rv.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.parseInt(fetchMaxWait));

        String fetchMaxPartitionBytes = opts.getOrDefault("kafka.max.partition.fetch.bytes", "1024");
        rv.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchMaxPartitionBytes);

        return rv;
    }

    private Map<String, String> parseKafkaExecutorConfig(Map<String, String> opts) {
        // executor specific config
        Map<String, String> rv = new HashMap<>();
        // if running in unit-test mode
        String useMockKafkaConsumer = opts.getOrDefault("kafka.useMockKafkaConsumer", "false");
        rv.put("useMockKafkaConsumer", useMockKafkaConsumer);

        // cut-off epoch
        String inludeEpochAndAfterString = opts
                .getOrDefault("kafka.includeEpochAndAfter", String.valueOf(Long.MIN_VALUE / (1000 * 1000)));
        rv.put("includeEpochAndAfter", inludeEpochAndAfterString);

        return rv;
    }
}
