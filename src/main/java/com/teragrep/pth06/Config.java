/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
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

package com.teragrep.pth06;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * <h2>Config</h2>
 * Holds all the settings for the program. Including MariaDB, Kafka and Spark.
 *
 * @since 17/08/2021
 * @author Mikko Kortelainen
 */
public final class Config {

    // scheduler
	public enum SchedulerType {
		BATCH,
		NOOP
	}

    // mariadb
    private final String DBuserName;
    private final String DBpassword;
    private final String DBurl;
    private final String DBjournaldbname;
    private final String DBstreamdbname;

    // s3
    private final String S3endPoint;
    private final String S3identity;
    private final String S3credential;

    // Partition handler count
    private final int NUM_PARTITIONS;

    // partition (quantum) length, for BatchScheduler
    private final float quantumLength;
    private final SchedulerType schedulerType;

    // Audit information
    private final String TeragrepAuditQuery;
    private final String TeragrepAuditReason;
    private final String TeragrepAuditUser;
    private final String TeragrepAuditPluginClassName;

    private final String query;

    private final boolean archiveEnabled;
    private final boolean kafkaEnabled;

    private final String kafkaConsumerGroupId;
    private final Map<String, Object> kafkaExecutorOpts;
    private final Map<String, Object> kafkaDriverOpts;
    private final Map<String, String> kafkaExecutorConfig;

    private final long archiveIncludeBeforeEpoch;

    private final boolean kafkaContinuousProcessing;

    private final boolean hideDatabaseExceptions;

    private final boolean skipNonRFC5424Files;

    public Config(DataSourceOptions opts) {

        S3endPoint = opts.get("S3endPoint").orElseThrow(() ->
                new IllegalArgumentException("missing S3endPoint")
        );

        S3identity = opts.get("S3identity").orElseThrow(() ->
                new IllegalArgumentException("missing S3identity")
        );

        S3credential = opts.get("S3credential").orElseThrow(() ->
                new IllegalArgumentException("missing S3credential")
        );

        DBuserName = opts.get("DBusername").orElseThrow(() ->
                new IllegalArgumentException("missing DBusername")
        );

        DBpassword = opts.get("DBpassword").orElseThrow(() ->
                new IllegalArgumentException("missing DBpassword")
        );

        DBurl = opts.get("DBurl").orElseThrow(() ->
                new IllegalArgumentException("missing DBurl")
        );

        DBjournaldbname = opts.get("DBjournaldbname").orElse("journaldb");

        DBstreamdbname = opts.get("DBstreamdbname").orElse("streamdb");

        // default value
	    // TODO activeSession.get.sparkContext.defaultParallelism;
        NUM_PARTITIONS = opts.getInt("num_partitions", 1);

        quantumLength = opts.getInt("quantumLength", 15);

        String schedulerString = opts.get("scheduler").orElse("BatchScheduler");
        if ("NoOpScheduler".equals(schedulerString)) {
            schedulerType = SchedulerType.NOOP;
        }
        else if ("BatchScheduler".equals(schedulerString)) {
            schedulerType = SchedulerType.BATCH;
        }
        else {
            throw new IllegalArgumentException("only NoOpScheduler and BatchScheduler are supported schedulers");
        }

        this.query = opts.get("queryXML").orElseThrow(() ->
                new IllegalArgumentException("missing queryXML")
        );

        TeragrepAuditQuery = opts.get("TeragrepAuditQuery").orElse(query);

        TeragrepAuditReason = opts.get("TeragrepAuditReason").orElse("");

        TeragrepAuditUser = opts.get("TeragrepAuditUser").orElse(
                System.getProperty("user.name")
        );

        if (!System.getProperty("user.name").equals(TeragrepAuditUser)) {
            throw new IllegalArgumentException("TeragrepAuditUser does not match user.name");
        }

        TeragrepAuditPluginClassName = opts.get("TeragrepAuditPluginClassName")
                .orElse("com.teragrep.rad_01.DefaultAuditPlugin");

        String archiveIncludeBeforeEpochString = opts.get("archive.includeBeforeEpoch").orElse(String.valueOf(Long.MAX_VALUE));
        archiveIncludeBeforeEpoch = Long.parseLong(archiveIncludeBeforeEpochString);

        String archiveEnabledString = opts.get("archive.enabled").orElse("true");
        archiveEnabled = "true".equals(archiveEnabledString);

        String kafkaEnabledString = opts.get("kafka.enabled").orElse("false");
        kafkaEnabled = "true".equals(kafkaEnabledString);

        if (kafkaEnabled) {
            kafkaConsumerGroupId = UUID.randomUUID().toString();
            kafkaDriverOpts = parseKafkaDriverOpts(opts);
            kafkaExecutorOpts = parseKafkaExecutorOpts(opts);
            kafkaExecutorConfig = parseKafkaExecutorConfig(opts);

            String kafkaContinuousProcessingString = opts.get("kafka.continuousProcessing").orElse("false");
            kafkaContinuousProcessing = "true".equals(kafkaContinuousProcessingString);
        }
        else {
            kafkaConsumerGroupId = null;
            kafkaDriverOpts = null;
            kafkaExecutorOpts = null;
            kafkaExecutorConfig = null;
            kafkaContinuousProcessing = false;
        }

        // hide exceptions
        String hideDatabaseExceptionsString =
                opts.get("hideDatabaseExceptions").orElse(
                "false");
        hideDatabaseExceptions =
                "true".equals(hideDatabaseExceptionsString);

        // skip not rfc5424 parseable files
        String skipNonRFC5424FilesString =
                opts.get("skipNonRFC5424Files").orElse(
                        "false");
        skipNonRFC5424Files =
                "true".equals(skipNonRFC5424FilesString);
    }

    private Map<String, Object> parseKafkaCommonOpts(DataSourceOptions opts) {
        // options common for driver and executor
        Map<String, Object> rv = new HashMap<>();
        String bootstrapServers = opts.get("kafka.bootstrap.servers").orElseThrow(() ->
                new IllegalArgumentException("missing kafka.bootstrap.servers"));
        rv.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String saslMechanism = opts.get("kafka.sasl.mechanism").orElseThrow(() ->
                new IllegalArgumentException("missing kafka.sasl.mechanism"));
        rv.put(SaslConfigs.SASL_MECHANISM, saslMechanism);


        String securityProtocol = opts.get("kafka.security.protocol").orElseThrow(() ->
                new IllegalArgumentException("missing kafka.security.protocol"));
            rv.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        String jaasConfig = opts.get("kafka.sasl.jaas.config").orElseThrow(() ->
                new IllegalArgumentException("missing kafka.sasl.jaas.config"));
        rv.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        // hardcoded values
        rv.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        rv.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        rv.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        rv.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536);
        rv.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 86400000); // avoid interrupt

        return rv;
    }

    private Map<String, Object> parseKafkaDriverOpts(DataSourceOptions opts) {
        // driver specific options
        Map<String, Object> rv = parseKafkaCommonOpts(opts);
        rv.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId + "-driver");
        rv.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        rv.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // driver should not

        return rv;
    }

    private Map<String, Object> parseKafkaExecutorOpts(DataSourceOptions opts) {
        // executor specific options
        Map<String, Object> rv = parseKafkaCommonOpts(opts);

        rv.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId + "-executor");
        rv.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String maxPollRecords = opts.get("kafka.max.poll.records").orElse("1000");
        rv.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(maxPollRecords));

        String fetchMaxBytes = opts.get("kafka.fetch.max.bytes").orElse("1024");
        rv.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.parseInt(fetchMaxBytes));

        String fetchMaxWait = opts.get("kafka.fetch.max.wait.ms").orElse(String.valueOf(120*1000));
        rv.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.parseInt(fetchMaxWait));

        String fetchMaxPartitionBytes = opts.get("kafka.max.partition.fetch.bytes").orElse("1024");
        rv.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchMaxPartitionBytes);

        return rv;
    }

    private Map<String, String> parseKafkaExecutorConfig(DataSourceOptions opts) {
        // executor specific config
        Map<String, String> rv = new HashMap<>();
        // if running in unit-test mode
        String useMockKafkaConsumer = opts.get("kafka.useMockKafkaConsumer").orElse("false");
        rv.put("useMockKafkaConsumer", useMockKafkaConsumer);

        // cut-off epoch
        String inludeEpochAndAfterString = opts.get("kafka.includeEpochAndAfter").orElse(String.valueOf(Long.MIN_VALUE/(1000*1000)));
        rv.put("includeEpochAndAfter", inludeEpochAndAfterString);

        return rv;
    }

    public int getNUM_PARTITIONS() {
        return NUM_PARTITIONS;
    }

    public float getQuantumLength() {return  quantumLength;}

    public SchedulerType getSchedulerType() {return schedulerType;}

    public String getDBjournaldbname() {
        return DBjournaldbname;
    }

    public String getDBpassword() {
        return DBpassword;
    }

    public String getDBstreamdbname() {
        return DBstreamdbname;
    }

    public String getDBurl() {
        return DBurl;
    }

    public String getDBuserName() {
        return DBuserName;
    }

    public String getQuery() {
        return query;
    }

    public String getS3credential() {
        return S3credential;
    }

    public String getS3endPoint() {
        return S3endPoint;
    }

    public String getS3identity() {
        return S3identity;
    }

    public String getTeragrepAuditQuery() {
        return TeragrepAuditQuery;
    }

    public String getTeragrepAuditReason() {
        return TeragrepAuditReason;
    }

    public String getTeragrepAuditUser() {
        return TeragrepAuditUser;
    }

    public String getTeragrepAuditPluginClassName() {
        return TeragrepAuditPluginClassName;
    }

    public Map<String, Object> getKafkaDriverOpts () {
        return kafkaDriverOpts;
    }

    public Map<String, Object> getKafkaExecutorOpts() {
        return kafkaExecutorOpts;
    }

    public Map<String, String> getKafkaExecutorConfig() {
        return kafkaExecutorConfig;
    }

    public boolean isArchiveEnabled() {
        return archiveEnabled;
    }

    public boolean isKafkaEnabled() {
        return kafkaEnabled;
    }

    public long getArchiveIncludeBeforeEpoch() {
        return archiveIncludeBeforeEpoch;
    }


    public boolean getKafkaContinuousProcessing() {
        return kafkaContinuousProcessing;
    }


    public boolean getHideDatabaseExceptions() {
        return hideDatabaseExceptions;
    }

    public boolean getSkipNonRFC5424Files() {
        return skipNonRFC5424Files;
    }

}
