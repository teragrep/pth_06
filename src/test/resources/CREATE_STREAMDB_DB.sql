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

CREATE DATABASE streamdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE bloomdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
grant all privileges on streamdb.* to streamdb@'%' identified by 'streamdb_pass';
grant all privileges on bloomdb.* to streamdb@'%' identified by 'streamdb_pass';
USE streamdb;
CREATE TABLE `log_group` (
                             `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                             `name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
                             PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE `host` (
                        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                        `name` varchar(175) COLLATE utf8mb4_unicode_ci NOT NULL,
                        `gid` int(10) unsigned NOT NULL,
                        PRIMARY KEY (`id`),
                        KEY `gid` (`gid`),
                        KEY `idx_name_id` (`name`,`id`),
                        CONSTRAINT `host_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=523 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE `stream` (
                          `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                          `gid` int(10) unsigned NOT NULL,
                          `directory` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                          `stream` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                          `tag` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL,
                          PRIMARY KEY (`id`),
                          KEY `gid` (`gid`),
                          CONSTRAINT `stream_ibfk_1` FOREIGN KEY (`gid`) REFERENCES `log_group` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO log_group (id, name) VALUES (1, 'testGroup1');
INSERT INTO stream (id, gid, directory, stream, tag) VALUES (1, 1, 'example', 'log:example:0', 'example');
INSERT INTO host (id, name, gid) VALUES (1, 'testHost1', 1);
USE journaldb;
CREATE TABLE `source_system` (
                                 `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
                                 `name` varchar(175) NOT NULL COMMENT 'Source system''s name',
                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `uix_source_system_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=113 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different applications.';
CREATE TABLE `category` (
                            `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
                            `name` varchar(175) DEFAULT NULL COMMENT 'Category''s name',
                            PRIMARY KEY (`id`),
                            UNIQUE KEY `uix_category_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different categories.';
CREATE TABLE `bucket` (
                          `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
                          `name` varchar(64) NOT NULL COMMENT 'Name of the bucket',
                          PRIMARY KEY (`id`),
                          UNIQUE KEY `uix_bucket_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=92 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Buckets in object storage';
CREATE TABLE `host` (
                        `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
                        `name` varchar(175) NOT NULL COMMENT 'Name of the host',
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uix_host_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Host names';
CREATE TABLE `logfile` (
                           `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
                           `logdate` date NOT NULL COMMENT 'Log file''s date',
                           `expiration` date NOT NULL COMMENT 'Log file''s expiration date',
                           `bucket_id` smallint(5) unsigned NOT NULL COMMENT 'Reference to bucket table',
                           `path` varchar(2048) NOT NULL COMMENT 'Log file''s path in object storage',
                           `object_key_hash` char(64) GENERATED ALWAYS AS (sha2(concat(`path`,`bucket_id`),256)) STORED COMMENT 'Hash of path and bucket_id for uniqueness checks. Known length: 64 characters (SHA-256)',
                           `host_id` smallint(5) unsigned NOT NULL COMMENT 'Reference to host table',
                           `original_filename` varchar(255) NOT NULL COMMENT 'Log file''s original file name',
                           `archived` datetime NOT NULL COMMENT 'Date and time when the log file was archived',
                           `file_size` bigint(20) unsigned NOT NULL DEFAULT 0 COMMENT 'Log file''s size in bytes',
                           `sha256_checksum` char(44) NOT NULL COMMENT 'An SHA256 hash of the log file (Note: known to be 44 characters long)',
                           `archive_etag` varchar(64) NOT NULL COMMENT 'Object storage''s MD5 hash of the log file (Note: room left for possible implementation changes)',
                           `logtag` varchar(48) NOT NULL COMMENT 'A link back to CFEngine',
                           `source_system_id` smallint(5) unsigned NOT NULL COMMENT 'Log file''s source system (references source_system.id)',
                           `category_id` smallint(5) unsigned NOT NULL DEFAULT 0 COMMENT 'Log file''s category (references category.id)',
                           `uncompressed_file_size` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  uncompressed file size',
                           `epoch_hour` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch logdate',
                           `epoch_expires` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch expiration',
                           `epoch_archived` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s  epoch archived',
                           PRIMARY KEY (`id`),
                           UNIQUE KEY `uix_logfile_object_hash` (`object_key_hash`),
                           KEY `bucket_id` (`bucket_id`),
                           KEY `category_id` (`category_id`),
                           KEY `ix_logfile_expiration` (`expiration`),
                           KEY `ix_logfile__source_system_id` (`source_system_id`),
                           KEY `cix_logfile_logdate_host_id_logtag` (`logdate`,`host_id`,`logtag`),
                           KEY `cix_logfile_host_id_logtag_logdate` (`host_id`,`logtag`,`logdate`),
                           KEY `cix_logfile_epoch_hour_host_id_logtag` (`epoch_hour`,`host_id`,`logtag`),
                           KEY `ix_logfile_epoch_expires` (`epoch_expires`),
                           CONSTRAINT `fk_logfile__source_system_id` FOREIGN KEY (`source_system_id`) REFERENCES `source_system` (`id`),
                           CONSTRAINT `logfile_ibfk_1` FOREIGN KEY (`bucket_id`) REFERENCES `bucket` (`id`),
                           CONSTRAINT `logfile_ibfk_2` FOREIGN KEY (`host_id`) REFERENCES `host` (`id`),
                           CONSTRAINT `logfile_ibfk_4` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=135 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for log files that have been run through Log Archiver';
INSERT INTO host (id, name) VALUES (1, 'testHost1');
INSERT INTO bucket (id, name) VALUES (1, 'bucket1');
INSERT INTO category (id, name) VALUES (1, 'testCategory');
INSERT INTO source_system (id, name) VALUES (2, 'testSourceSystem2');
flush privileges;