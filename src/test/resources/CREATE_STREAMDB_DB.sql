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
flush privileges;