package com.github.michaelzhao820.distributedlog.internal.log;

import lombok.Getter;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Index {
    private static final int OFF_WIDTH = 4;
    private static final int POS_WIDTH = 8;
    public static final int ENT_WIDTH = OFF_WIDTH + POS_WIDTH;

    private final File initialFile;
    private final RandomAccessFile file;
    private final MappedByteBuffer mmap;
    @Getter
    private long size; // total bytes used (just entries, no header)
    private final Config config;

    public record IndexEntry(int offset, long pos) {}

    public Index(File file, Config c) throws IOException {
        this.initialFile = file;
        this.file = new RandomAccessFile(file, "rw");

        this.size = file.length();

        if (file.length() < c.segment.maxIndexBytes) {
            this.file.setLength(c.segment.maxIndexBytes);
        }

        FileChannel channel = this.file.getChannel();
        long mmapSize = c.segment.maxIndexBytes;
        this.mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, mmapSize);
        this.config = c;
    }

    public void close() throws IOException {
        mmap.force();
        file.getFD().sync();
        file.setLength(size);
        file.close();
    }

    public IndexEntry read(long in) throws EOFException {
        if (size == 0) {
            throw new EOFException("EOF: index is empty");
        }

        long outOffset = (in == -1) ? (size / ENT_WIDTH) - 1 : in;
        long pos = outOffset * ENT_WIDTH;

        if (size < pos + ENT_WIDTH) {
            throw new EOFException("EOF: requested offset beyond last entry");
        }

        int off = mmap.getInt((int) pos);
        long position = mmap.getLong((int) (pos + OFF_WIDTH));

        return new IndexEntry(off, position);
    }

    public void write(int off, long pos) throws EOFException {
        if (size + ENT_WIDTH > config.segment.maxIndexBytes) {
            throw new EOFException("Index full");
        }

        mmap.putInt((int) size, off);
        mmap.putLong((int) (size + OFF_WIDTH), pos);
        size += ENT_WIDTH;

        mmap.force();
    }

    public String name() {
        return this.initialFile.getAbsolutePath();
    }
}
