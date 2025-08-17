package com.github.michaelzhao820.distributedlog.internal.log;

import com.github.michaelzhao820.distributedlog.api.v1.LogProto;
import org.junit.jupiter.api.*;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class SegmentTest {

    private File tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = new File(System.getProperty("java.io.tmpdir"), "segment-test-" + System.nanoTime());
        if (!tempDir.mkdir()) {
            throw new IOException("Failed to create temp dir");
        }
    }

    @AfterEach
    void tearDown() {
        deleteDirectory(tempDir);
    }

    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }

    @Test
    void testSegment() throws IOException {
        LogProto.Record want = LogProto.Record.newBuilder()
                .setValue(com.google.protobuf.ByteString.copyFromUtf8("hello world"))
                .build();

        Config c = new Config();
        c.segment.maxStoreBytes = 1024;
        c.segment.maxIndexBytes = 36;

        Segment s = new Segment(tempDir.getAbsolutePath(), 16, c);

        assertEquals(16, s.getNextOffset());
        assertFalse(s.isMaxed());

        for (long i = 0; i < 3; i++) {
            long off = s.append(want);
            assertEquals(16 + i, off);

            LogProto.Record got = s.read(off);
            assertArrayEquals(want.getValue().toByteArray(), got.getValue().toByteArray());
        }

        try {
            s.append(want);
            fail("Expected EOFException");
        } catch (EOFException e) {
            assertEquals("Index full", e.getMessage());
        }

        assertTrue(s.isMaxed(), "Segment should be maxed after index full");

        c.segment.maxStoreBytes = want.getValue().size() * 3L;
        c.segment.maxIndexBytes = 1024;
        Segment s2 = new Segment(tempDir.getAbsolutePath(), 16, c);
        assertTrue(s2.isMaxed(), "Segment should be maxed due to store size");

        s2.remove();
        Segment s3 = new Segment(tempDir.getAbsolutePath(), 16, c);
        assertFalse(s3.isMaxed(), "New segment should not be maxed");
    }
}