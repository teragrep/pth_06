package com.teragrep.pth_06.planner;

import java.nio.ByteBuffer;

public final class EpochFromRowKey {
    private final byte[] rowKeyBytes;
    private final int expectedRowKeyLength;
    private final int epochValueIndex;

    public EpochFromRowKey(final byte[] rowKeyBytes) {
        this(rowKeyBytes, 24, 8);
    }

    private EpochFromRowKey(final byte[] rowKeyBytes, final int expectedRowKeyLength, final int epochValueIndex) {
        this.rowKeyBytes = rowKeyBytes;
        this.expectedRowKeyLength = expectedRowKeyLength;
        this.epochValueIndex = epochValueIndex;
    }

    public long epoch() {
        if (rowKeyBytes.length == expectedRowKeyLength) {
            return ByteBuffer.wrap(rowKeyBytes).getLong(epochValueIndex);
        }
        throw new IllegalArgumentException("Malformatted row key bytes, length was not expected <" + expectedRowKeyLength + ">");
    }
}
