package com.github.michaelzhao820.distributedlog.internal.log;

import com.github.michaelzhao820.distributedlog.api.v1.LogProto;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogTest {

    private File tempDir;
    private Config config;
    private Log log;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("store-test").toFile();
        config = new Config();
        config.segment.maxStoreBytes = 32;
        log = new Log(tempDir.getAbsolutePath(), config);
    }

    @AfterEach
    void tearDown() throws IOException {
        log.remove();
        if (tempDir.exists()) {
            tempDir.delete();
        }
    }

    @Test
    void appendAndReadRecord() throws IOException {
        LogProto.Record record = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFrom("hello world".getBytes()))
                .build();

        long offset = log.append(record);
        assertEquals(0, offset);

        LogProto.Record read = log.read(offset);
        assertArrayEquals(record.getValue().toByteArray(), read.getValue().toByteArray());
    }

    @Test
    void offsetOutOfRangeThrows() {
        Exception exception = assertThrows(IOException.class, () -> log.read(1));
        assertTrue(exception.getMessage().contains("Offset out of range"));
    }

    @Test
    void initWithExistingSegments() throws IOException {
        LogProto.Record record = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFrom("hello world".getBytes()))
                .build();

        for (int i = 0; i < 3; i++) {
            log.append(record);
        }

        long lowest = log.lowestOffset();
        long highest = log.highestOffset();
        assertEquals(0, lowest);
        assertEquals(2, highest);

        Log newLog = new Log(tempDir.getAbsolutePath(), config);
        assertEquals(0, newLog.lowestOffset());
        assertEquals(2, newLog.highestOffset());
        newLog.close();
    }

    @Test
    void readerReturnsFullLog() throws IOException {
        LogProto.Record record1 = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFrom("hello world".getBytes()))
                .build();
        LogProto.Record record2 = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFrom("second record".getBytes()))
                .build();

        log.append(record1);
        log.append(record2);

        InputStream reader = log.reader();
        byte[] rawBytes = reader.readAllBytes();

        int lenWidth = 8;
        int pos = 0;

        int len1 = (int) bytesToLong(rawBytes, pos);
        pos += lenWidth;
        byte[] recordBytes1 = new byte[len1];
        System.arraycopy(rawBytes, pos, recordBytes1, 0, len1);
        LogProto.Record read1 = LogProto.Record.parseFrom(recordBytes1);
        assertArrayEquals(record1.getValue().toByteArray(), read1.getValue().toByteArray());
        pos += len1;

        int len2 = (int) bytesToLong(rawBytes, pos);
        pos += lenWidth;
        byte[] recordBytes2 = new byte[len2];
        System.arraycopy(rawBytes, pos, recordBytes2, 0, len2);
        LogProto.Record read2 = LogProto.Record.parseFrom(recordBytes2);
        assertArrayEquals(record2.getValue().toByteArray(), read2.getValue().toByteArray());
    }

    private long bytesToLong(byte[] b, int offset) {
        return ((b[offset] & 0xFFL) << 56)
                | ((b[offset + 1] & 0xFFL) << 48)
                | ((b[offset + 2] & 0xFFL) << 40)
                | ((b[offset + 3] & 0xFFL) << 32)
                | ((b[offset + 4] & 0xFFL) << 24)
                | ((b[offset + 5] & 0xFFL) << 16)
                | ((b[offset + 6] & 0xFFL) << 8)
                | (b[offset + 7] & 0xFFL);
    }
    @Test
    void truncateRemovesRecordsBelowOffset() throws IOException {
        LogProto.Record record = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFrom("hello world".getBytes()))
                .build();

        for (int i = 0; i < 3; i++) {
            log.append(record);
        }

        log.truncate(1);

        assertThrows(IOException.class, () -> log.read(0));
    }
}
