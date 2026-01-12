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

-- MariaDB dump 10.19  Distrib 10.11.11-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: journaldb
-- ------------------------------------------------------
-- Server version	10.11.11-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bucket`
--

DROP TABLE IF EXISTS `bucket`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `bucket` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL COMMENT 'Name of the bucket',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_bucket_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=147 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Buckets in object storage';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `bucket`
--

LOCK TABLES `bucket` WRITE;
/*!40000 ALTER TABLE `bucket` DISABLE KEYS */;
/*!40000 ALTER TABLE `bucket` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `category`
--

DROP TABLE IF EXISTS `category`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `category` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(175) DEFAULT NULL COMMENT 'Category''s name',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_category_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=192 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different categories.';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `category`
--

LOCK TABLES `category` WRITE;
/*!40000 ALTER TABLE `category` DISABLE KEYS */;
/*!40000 ALTER TABLE `category` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ci`
--

DROP TABLE IF EXISTS `ci`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ci` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID for ci',
  `name` varchar(255) NOT NULL COMMENT 'Configuration item name of the logfile records',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_ci` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Contains ci values that are identified using the ID';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ci`
--

LOCK TABLES `ci` WRITE;
/*!40000 ALTER TABLE `ci` DISABLE KEYS */;
/*!40000 ALTER TABLE `ci` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `corrupted_archive`
--

DROP TABLE IF EXISTS `corrupted_archive`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `corrupted_archive` (
  `logfile_id` bigint(20) unsigned NOT NULL COMMENT 'The logfile that is the corrupted archive (references logfile.id).',
  PRIMARY KEY (`logfile_id`),
  CONSTRAINT `corrupted_archive_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Contains logfiles that are both archives and detected to be corrupted.';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `corrupted_archive`
--

LOCK TABLES `corrupted_archive` WRITE;
/*!40000 ALTER TABLE `corrupted_archive` DISABLE KEYS */;
/*!40000 ALTER TABLE `corrupted_archive` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `flyway_schema_history`
--

DROP TABLE IF EXISTS `flyway_schema_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `flyway_schema_history` (
  `installed_rank` int(11) NOT NULL,
  `version` varchar(50) DEFAULT NULL,
  `description` varchar(200) NOT NULL,
  `type` varchar(20) NOT NULL,
  `script` varchar(1000) NOT NULL,
  `checksum` int(11) DEFAULT NULL,
  `installed_by` varchar(100) NOT NULL,
  `installed_on` timestamp NOT NULL DEFAULT current_timestamp(),
  `execution_time` int(11) NOT NULL,
  `success` tinyint(1) NOT NULL,
  PRIMARY KEY (`installed_rank`),
  KEY `flyway_schema_history_s_idx` (`success`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `flyway_schema_history`
--

LOCK TABLES `flyway_schema_history` WRITE;
/*!40000 ALTER TABLE `flyway_schema_history` DISABLE KEYS */;
/*!40000 ALTER TABLE `flyway_schema_history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `host`
--

DROP TABLE IF EXISTS `host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `host` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(175) NOT NULL COMMENT 'Name of the host',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_host_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=192 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Host names';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `host`
--

LOCK TABLES `host` WRITE;
/*!40000 ALTER TABLE `host` DISABLE KEYS */;
/*!40000 ALTER TABLE `host` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `logfile`
--

DROP TABLE IF EXISTS `logfile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
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
  `ci_id` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s foreign key to ci table',
  `logtag_id` bigint(20) unsigned NOT NULL COMMENT 'Log file''s foreign key to logtag',
  `object_format_id` bigint(20) unsigned DEFAULT NULL COMMENT 'Log file''s foreign key to object_format table',
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
  KEY `fk_logfile__ci_id` (`ci_id`),
  KEY `fk_logfile__logtag_id` (`logtag_id`),
  KEY `cix_logfile_epoch_hour_host_id_logtag_id` (`epoch_hour`,`host_id`,`logtag_id`),
  KEY `cix_logfile_logdate_host_id_logtag_id` (`logdate`,`host_id`,`logtag_id`),
  KEY `fk_logfile__object_format_id` (`object_format_id`),
  CONSTRAINT `fk_logfile__ci_id` FOREIGN KEY (`ci_id`) REFERENCES `ci` (`id`),
  CONSTRAINT `fk_logfile__logtag_id` FOREIGN KEY (`logtag_id`) REFERENCES `logtag` (`id`),
  CONSTRAINT `fk_logfile__object_format_id` FOREIGN KEY (`object_format_id`) REFERENCES `object_format` (`id`),
  CONSTRAINT `fk_logfile__source_system_id` FOREIGN KEY (`source_system_id`) REFERENCES `source_system` (`id`),
  CONSTRAINT `logfile_ibfk_1` FOREIGN KEY (`bucket_id`) REFERENCES `bucket` (`id`),
  CONSTRAINT `logfile_ibfk_2` FOREIGN KEY (`host_id`) REFERENCES `host` (`id`),
  CONSTRAINT `logfile_ibfk_4` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=289 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for log files that have been run through Log Archiver';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `logfile`
--

LOCK TABLES `logfile` WRITE;
/*!40000 ALTER TABLE `logfile` DISABLE KEYS */;
/*!40000 ALTER TABLE `logfile` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `logtag`
--

DROP TABLE IF EXISTS `logtag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `logtag` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID for logtag',
  `logtag` varchar(48) NOT NULL COMMENT 'A link back to CFEngine',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_logtag` (`logtag`)
) ENGINE=InnoDB AUTO_INCREMENT=289 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Contains logtag values that are identified using the ID';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `logtag`
--

LOCK TABLES `logtag` WRITE;
/*!40000 ALTER TABLE `logtag` DISABLE KEYS */;
/*!40000 ALTER TABLE `logtag` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metadata_value`
--

DROP TABLE IF EXISTS `metadata_value`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `metadata_value` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `logfile_id` bigint(20) unsigned NOT NULL COMMENT 'Foreign key referencing Logfile.id',
  `value_key` varchar(255) NOT NULL COMMENT 'Identifier key for the attribute',
  `value` varchar(255) NOT NULL COMMENT 'Value of the attribute',
  PRIMARY KEY (`id`),
  KEY `logfile_id` (`logfile_id`),
  CONSTRAINT `metadata_value_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=67 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains additional attributes for logfiles. The attributes do not apply to each logfile and therefore are not columns in Logfile table';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metadata_value`
--

LOCK TABLES `metadata_value` WRITE;
/*!40000 ALTER TABLE `metadata_value` DISABLE KEYS */;
/*!40000 ALTER TABLE `metadata_value` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `object_format`
--

DROP TABLE IF EXISTS `object_format`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `object_format` (
                                 `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID for object_format',
                                 `name` varchar(255) NOT NULL COMMENT 'object_format of the logfile records',
                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `uix_object_format` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Contains object_format values that are identified using the ID';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `object_format`
--

LOCK TABLES `object_format` WRITE;
/*!40000 ALTER TABLE `object_format` DISABLE KEYS */;
/*!40000 ALTER TABLE `object_format` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `restore_job`
--

DROP TABLE IF EXISTS `restore_job`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `restore_job` (
  `job_id` varchar(768) NOT NULL COMMENT 'Job id from aws glacier',
  `logfile_id` bigint(20) unsigned NOT NULL COMMENT 'Reference to logfile which is going to be restored',
  `created` datetime NOT NULL COMMENT 'Job creation time',
  `task_id` varchar(5) DEFAULT NULL COMMENT 'Task id this job belongs to',
  PRIMARY KEY (`job_id`),
  KEY `logfile_id` (`logfile_id`),
  CONSTRAINT `restore_job_ibfk_1` FOREIGN KEY (`logfile_id`) REFERENCES `logfile` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Glacier restoration jobs';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `restore_job`
--

LOCK TABLES `restore_job` WRITE;
/*!40000 ALTER TABLE `restore_job` DISABLE KEYS */;
/*!40000 ALTER TABLE `restore_job` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `source_system`
--

DROP TABLE IF EXISTS `source_system`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `source_system` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(175) NOT NULL COMMENT 'Source system''s name',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_source_system_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=193 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Contains information for different applications.';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `source_system`
--

LOCK TABLES `source_system` WRITE;
/*!40000 ALTER TABLE `source_system` DISABLE KEYS */;
/*!40000 ALTER TABLE `source_system` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-23 14:57:42
