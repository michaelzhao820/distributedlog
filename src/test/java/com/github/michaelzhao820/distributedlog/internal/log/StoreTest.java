package com.github.michaelzhao820.distributedlog.internal.log;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {

    private static final byte[] WRITE = "hello world".getBytes();
    private static final int LEN_WIDTH = 8;
    private static final long WIDTH = WRITE.length + LEN_WIDTH;

    @Test
    void testStoreAppendRead() throws IOException {
        File tempFile = Files.createTempFile("store_append_read_test", null).toFile();
        tempFile.deleteOnExit();

        Store s = new Store(tempFile);

        testAppend(s);
        testRead(s);
        testReadAt(s);

        // reopen store to test recovery
        s = new Store(tempFile);
        testRead(s);
        s.close();
    }

    private void testAppend(Store s) {
        long expectedPos = 0;
        for (int i = 1; i < 4; i++) {
            Store.AppendResult result = s.append(WRITE);
            assertEquals(WIDTH * i, result.pos() + result.n());
        }
    }

    private void testRead(Store s) {
        long pos = 0;
        for (int i = 1; i < 4; i++) {
            byte[] read = s.read(pos);
            assertArrayEquals(WRITE, read);
            pos += WIDTH;
        }
    }

    private void testReadAt(Store s) {
        long off = 0;
        for (int i = 1; i < 4; i++) {
            byte[] lenBytes = new byte[LEN_WIDTH];
            int n = s.readAt(lenBytes, off);
            assertEquals(LEN_WIDTH, n);

            off += n;
            long size = bytesToLong(lenBytes);
            byte[] data = new byte[(int) size];
            n = s.readAt(data, off);
            assertEquals(size, n);
            assertArrayEquals(WRITE, data);

            off += n;
        }
    }

    @Test
    void testStoreClose() throws IOException {
        File tempFile = Files.createTempFile("store_close_test", null).toFile();
        tempFile.deleteOnExit();

        Store s = new Store(tempFile);
        s.append(WRITE);

        long beforeSize = tempFile.length();
        s.close();
        long afterSize = tempFile.length();

        assertTrue(afterSize > beforeSize);
    }

    private long bytesToLong(byte[] bytes) {
        return java.nio.ByteBuffer.wrap(bytes)
                .order(java.nio.ByteOrder.BIG_ENDIAN)
                .getLong();
    }
}
