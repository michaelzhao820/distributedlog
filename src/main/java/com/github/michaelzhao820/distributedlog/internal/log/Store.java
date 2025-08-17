package com.github.michaelzhao820.distributedlog.internal.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class Store {

    private static final Logger logger = Logger.getLogger(Store.class.getName());

    private static final ByteOrder ENC = ByteOrder.BIG_ENDIAN;
    private static final int LEN_WIDTH = 8;

    private final File file;
    private final ReentrantLock lock;
    private final BufferedOutputStream buf;
    private long size;

    public Store(File file) throws IOException {
        this.file = file;
        this.lock = new ReentrantLock();
        this.buf = new BufferedOutputStream(new FileOutputStream(file, true));
        this.size = file.length();
    }

    public AppendResult append(byte[] p) {
        lock.lock();
        try {
            long pos = this.size;

            ByteBuffer lenBuf = ByteBuffer.allocate(LEN_WIDTH);
            lenBuf.order(ENC);
            lenBuf.putLong(p.length);
            buf.write(lenBuf.array());

            buf.write(p);

            int w = LEN_WIDTH + p.length;
            this.size += w;

            return new AppendResult(w, pos);
        } catch (IOException e) {
            logger.severe("Failed to append record at position " + this.size + ": " + e.getMessage());
            throw new RuntimeException("Append failed at position " + this.size, e);
        } finally {
            lock.unlock();
        }
    }

    public byte[] read(long pos) {
        lock.lock();
        try {
            buf.flush();

            byte[] sizeBytes = new byte[LEN_WIDTH];
            try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
                raf.seek(pos);
                raf.readFully(sizeBytes);

                // Decode the record size
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                sizeBuffer.order(ENC);
                long recordSize = sizeBuffer.getLong();

                byte[] record = new byte[(int) recordSize];
                raf.readFully(record);

                return record;
            }
        } catch (IOException e) {
            logger.severe("Failed to read record at position " + pos + ": " + e.getMessage());
            throw new RuntimeException("Read failed at position " + pos, e);
        } finally {
            lock.unlock();
        }
    }

    public int readAt(byte[] p, long off) {
        lock.lock();
        try {
            try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
                raf.seek(off);
                raf.readFully(p);
                return p.length;
            }
        } catch (IOException e) {
            logger.severe("Failed to read at offset " + off + ": " + e.getMessage());
            throw new RuntimeException("ReadAt failed at offset " + off, e);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            buf.flush();
            buf.close();
        } catch (IOException e) {
            logger.severe("Failed to close store: " + e.getMessage());
            throw new RuntimeException("Close failed", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Represents the result of appending a record to the log.
     *
     * @param n   Total number of bytes written for this record (length prefix + payload)
     * @param pos Starting position of this record in the log file
     */
    public record AppendResult(long n, long pos) {}
}
