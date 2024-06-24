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
 */;

DROP TABLE IF EXISTS `filter_expected_100000_fpp_001`;
DROP TABLE IF EXISTS `filter_expected_1000000_fpp_003`;
DROP TABLE IF EXISTS `filter_expected_2500000_fpp_005`;

CREATE TABLE `filter_expected_100000_fpp_001` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `partition_id` BIGINT(20) unsigned NOT NULL UNIQUE,
    `filter` LONGBLOB,
    CONSTRAINT `fk_smallfilter_partition`
        FOREIGN KEY (`partition_id`) REFERENCES `journaldb`.`logfile`(`id`) ON DELETE CASCADE,
    CONSTRAINT `pk_small`
        PRIMARY KEY (`id`)
)ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

CREATE TABLE `filter_expected_1000000_fpp_003` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `partition_id` BIGINT(20) unsigned NOT NULL UNIQUE ,
    `filter` LONGBLOB,
    CONSTRAINT `fk_mediumfilter_partition`
        FOREIGN KEY (`partition_id`) REFERENCES `journaldb`.`logfile`(`id`) ON DELETE CASCADE,
    CONSTRAINT `pk_medium`
        PRIMARY KEY (`id`)
)ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

CREATE TABLE `filter_expected_2500000_fpp_005` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `partition_id` BIGINT(20) unsigned NOT NULL UNIQUE ,
    `filter` LONGBLOB,
    CONSTRAINT `fk_largefilter_partition`
        FOREIGN KEY (`partition_id`) REFERENCES `journaldb`.`logfile`(`id`) ON DELETE CASCADE,
    CONSTRAINT `pk_large`
        PRIMARY KEY (`id`)
)ENGINE=InnoDB ROW_FORMAT=COMPRESSED;
