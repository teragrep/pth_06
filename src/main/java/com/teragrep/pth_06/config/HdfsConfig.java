package com.teragrep.pth_06.config;

import java.time.Instant;
import java.util.Map;

public final class HdfsConfig {

    // HDFS
    public final long cutoffOffset; // Represents the cutoff epoch which dictates which files should not be fetched from HDFS based on their timestamps.
    public final String hdfsPath; // Represents the working directory path in HDFS filesystem.
    public final String hdfsUri; // Represents the address of the HDFS server.
    public final boolean useMockHdfsDatabase; // Represents the configuration parameter if mock database should be used or not. Used only for testing.

    public final boolean isStub;
    public HdfsConfig(Map<String, String> opts) {
        cutoffOffset = Long.parseLong(opts.getOrDefault("hdfs.hdfsCutoffOffset", String.valueOf(Instant.now().toEpochMilli() - 72*3600000))); // Default is -72h from now
        hdfsPath = getOrThrow(opts, "hdfs.hdfsPath");
        hdfsUri = getOrThrow(opts, "hdfs.hdfsUri");
        String useMockHdfsDatabaseString = opts.getOrDefault("useMockHdfsDatabase", "false");
        useMockHdfsDatabase = "true".equals(useMockHdfsDatabaseString);
        isStub = false;
    }

    public HdfsConfig() {
        cutoffOffset = 0L;
        hdfsPath = "";
        hdfsUri = "";
        useMockHdfsDatabase = false;
        isStub = true;
    }

    private String getOrThrow(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Configuration item '" + key + "' was null!");
        }
        return value;
    }

}