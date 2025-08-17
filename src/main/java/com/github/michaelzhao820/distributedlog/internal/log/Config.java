package com.github.michaelzhao820.distributedlog.internal.log;

public class Config {
    public Segment segment = new Segment();

    public static class Segment {
        public long maxStoreBytes;   // Removed static
        public long maxIndexBytes;   // Removed static
        public long initialOffset;
    }
}
