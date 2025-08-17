package com.github.michaelzhao820.distributedlog.internal.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Log {

    public record Record(byte[] value, long offset) {}

    List<Record> records = new ArrayList<>();
    private final Lock lock = new ReentrantLock();

    public long append(byte[] value) {
        lock.lock();
        try {
            long offset = records.size();
            records.add(new Record(value, offset));
            return offset;
        } finally {
            lock.unlock();
        }
    }
    public Record read(long offset) {
        lock.lock();
        try {
            if (offset >= records.size()) {
                throw new IndexOutOfBoundsException("offset not found");
            }
            return records.get((int) offset);
        } finally {
            lock.unlock();
        }
    }
}
