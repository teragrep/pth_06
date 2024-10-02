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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ArchiveConfigTest {

    @Test
    public void testBloomEnabledDefaultOff() {
        Map<String, String> opts = options();
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertFalse(defaultConfig.bloomEnabled);
    }

    @Test
    public void testWithoutFiltersDefaultOff() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertFalse(defaultConfig.withoutFilters);
    }

    @Test
    public void testBloomEnabled() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertTrue(defaultConfig.bloomEnabled);
    }

    @Test
    public void testWithoutFilters() {
        Map<String, String> opts = options();
        opts.put("bloom.withoutFilters", "true");
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertTrue(defaultConfig.withoutFilters);
        Assertions.assertFalse(defaultConfig.bloomEnabled);
    }

    @Test
    public void testWithoutFiltersBloomEnabled() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        opts.put("bloom.withoutFilters", "true");
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertTrue(defaultConfig.withoutFilters);
        Assertions.assertTrue(defaultConfig.bloomEnabled);
    }

    @Test
    public void testWithoutFiltersDefaultBloomEnabled() {
        Map<String, String> opts = options();
        opts.put("bloom.enabled", "true");
        ArchiveConfig defaultConfig = new ArchiveConfig(opts);
        Assertions.assertFalse(defaultConfig.withoutFilters);
        Assertions.assertTrue(defaultConfig.bloomEnabled);
    }

    // provides minimal options needed to avoid exceptions
    private Map<String, String> options() {
        Map<String, String> opts = new HashMap<>();
        opts.put("S3endPoint", "S3endPoint");
        opts.put("S3identity", "S3identity");
        opts.put("S3credential", "S3credential");
        opts.put("DBusername", "DBusername");
        opts.put("DBpassword", "DBpassword");
        opts.put("DBurl", "DBurl");
        return opts;
    }
}
