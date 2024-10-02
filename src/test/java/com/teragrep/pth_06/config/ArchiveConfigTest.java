package com.teragrep.pth_06.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ArchiveConfigTest {

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
    private Map<String,String> options() {
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