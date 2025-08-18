package com.github.michaelzhao820.distributedlog.internal.server;

import com.github.michaelzhao820.distributedlog.api.v1.LogProto.Record;

public interface CommitLog {
    /**
     * Append a record to the log and return its offset.
     */
    long append(Record record) throws Exception;

    /**
     * Read a record from the log by offset.
     */
    Record read(long offset) throws Exception;
}