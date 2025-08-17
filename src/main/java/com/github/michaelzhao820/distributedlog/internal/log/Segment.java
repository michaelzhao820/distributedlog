package com.github.michaelzhao820.distributedlog.internal.log;

import com.github.michaelzhao820.distributedlog.api.v1.LogProto;
import lombok.Getter;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

public class Segment {
    @Getter
    private final Store store;
    private final Index index;
    @Getter
    private final long baseOffset;
    @Getter
    private long nextOffset;
    private final Config config;

    public Segment(String dir, long baseOffset, Config c) throws IOException {
        this.baseOffset = baseOffset;

        File storeFile = new File(dir, baseOffset + ".store");
        this.store = new Store(storeFile);

        File indexFile = new File(dir, baseOffset + ".index");
        this.index = new Index(indexFile, c);

        try {
            Index.IndexEntry last = index.read(-1);
            this.nextOffset = baseOffset + last.offset() + 1;
        } catch (IOException e) {
            this.nextOffset = baseOffset;
        }
        this.config = c;
    }

    public long append(LogProto.Record record) throws EOFException {

        long current = this.nextOffset;

        LogProto.Record updatedRecord = record.toBuilder()
                .setOffset(current)
                .build();

        byte[] p = updatedRecord.toByteArray();

        Store.AppendResult result = this.store.append(p);
        long pos = result.pos();

        index.write((int)(current - baseOffset), pos);

        this.nextOffset++;

        return current;
    }

    public LogProto.Record read(long offset) throws IOException {
        Index.IndexEntry index = this.index.read(offset - baseOffset);

        byte[] p = this.store.read(index.pos());

        try {
            return LogProto.Record.parseFrom(p);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("Failed to parse record", e);
        }
    }

    public boolean isMaxed() {
        return this.store.getSize() >= this.config.segment.maxStoreBytes ||
               this.index.getSize() >= this.config.segment.maxIndexBytes;
    }

  public void remove() throws IOException {
    close();

    File indexFile = new File(index.name());
    if (!indexFile.delete()) {
      throw new IOException("Failed to delete index file: " + indexFile.getAbsolutePath());
    }

    File storeFile = new File(store.name());
    if (!storeFile.delete()) {
      throw new IOException("Failed to delete store file: " + storeFile.getAbsolutePath());
    }
  }

    public void close() throws IOException {
        try {
            if (index != null) {
                index.close();
            }
        } catch (IOException e) {
            throw new IOException("Failed to close index", e);
        }

        if (store != null) {
            store.close();
        }
    }

    public static long nearestMultiple(long j, long k) {
        if (j >= 0) {
            return (j / k) * k;
        } else {
            return ((j - k + 1) / k) * k;
        }
    }

}
