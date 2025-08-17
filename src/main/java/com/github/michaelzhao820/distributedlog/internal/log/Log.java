package com.github.michaelzhao820.distributedlog.internal.log;

import com.github.michaelzhao820.distributedlog.api.v1.LogProto;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;


import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.Collections;


public class Log {
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final String dir;
    private final Config config;
    private Segment activeSegment;
    private final List<Segment> segments = new ArrayList<>();

    public Log(String dir, Config c) throws IOException {
        if (c.segment.maxStoreBytes == 0) {
            c.segment.maxStoreBytes = 1024;
        }

        if (c.segment.maxIndexBytes == 0) {
            c.segment.maxIndexBytes = 1024;
        }
        this.dir = dir;
        this.config = c;
        setup();
    }
    private void setup() throws IOException{
        File dirFile = new File(this.dir);
        File[] files = dirFile.listFiles();
        if (files == null) {
            throw new RuntimeException("Cannot read directory:" + this.dir);
        }
        List<Long> baseOffsets = new ArrayList<>();
        for (File file : files) {
            String name = file.getName();

            int dotIndex = name.lastIndexOf(".");
            if (dotIndex != -1) {
                name = name.substring(0, dotIndex);
            }
            try {
                long off = Long.parseLong(name);
                baseOffsets.add(off);
            } catch (NumberFormatException e) {

            }
        }
        Collections.sort(baseOffsets);

        for (int i = 0; i < baseOffsets.size(); i++) {
            long baseOffset = baseOffsets.get(i);
            newSegment(baseOffset);
            // baseOffset contains dup for index and store so we skip
            // the dup
            i++;
        }
        if (segments.isEmpty()) {
            newSegment(config.segment.initialOffset);
        }
    }

    public long append(LogProto.Record record) throws IOException {
        rwLock.writeLock().lock();
        try {
            long offset = activeSegment.append(record);
            if (activeSegment.isMaxed()) {
                newSegment(offset + 1);
            }
            return offset;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public LogProto.Record read(long offset) throws IOException {
        rwLock.readLock().lock();
        try {
            Segment s = null;
            for (Segment segment : segments) {
                if (segment.getBaseOffset() <= offset && offset < segment.getNextOffset()) {
                    s = segment;
                    break;
                }
            }
            if (s == null) {
                throw new IOException("Offset out of range: " + offset);
            }
            return s.read(offset);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void close() throws IOException {
        rwLock.writeLock().lock();
        try {
            for (Segment segment : segments) {
                segment.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void remove() throws IOException {
        close();

        File dirFile = new File(dir);
        if (dirFile.exists()) {
            deleteDirectoryRecursively(dirFile);
        }
        segments.clear();
        activeSegment = null;
    }

    public void reset() throws IOException {
        remove();
        setup();
    }

    public long lowestOffset() throws IOException {
        rwLock.readLock().lock();
        try {
            if (segments.isEmpty()) {
                throw new IOException("No segments available");
            }
            return segments.get(0).getBaseOffset();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long highestOffset() {
        rwLock.readLock().lock();
        try {
            if (segments.isEmpty()) {
                return 0;
            }
            long off = segments.get(segments.size() - 1).getNextOffset();
            if (off == 0) {
                return 0;
            }
            return off - 1;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void truncate(long lowest) throws IOException {
        rwLock.writeLock().lock();
        try {
            List<Segment> remainingSegments = new ArrayList<>();
            for (Segment s : segments) {
                if (s.getNextOffset() <= lowest + 1) {
                    s.remove();
                    continue;
                }
                remainingSegments.add(s);
            }
            segments.clear();
            segments.addAll(remainingSegments);
            if (!segments.isEmpty()) {
                activeSegment = segments.get(segments.size() - 1);
            } else {
                activeSegment = null;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public InputStream reader() throws IOException {
        rwLock.readLock().lock();
        try {
            Vector<InputStream> streams = new Vector<>();
            for (Segment segment : segments) {
                streams.add(segment.getStore().inputStream());
            }
            return new SequenceInputStream(streams.elements());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void deleteDirectoryRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDirectoryRecursively(child);
                }
            }
        }
        if (!file.delete()) {
            throw new IOException("Failed to delete file or directory: " + file.getAbsolutePath());
        }
    }

    private void newSegment(long offset) throws IOException {
        Segment s = new Segment(dir, offset, config);
        segments.add(s);
        activeSegment = s;
    }
}
