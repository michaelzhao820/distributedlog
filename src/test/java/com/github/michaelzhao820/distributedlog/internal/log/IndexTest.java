package com.github.michaelzhao820.distributedlog.internal.log;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

public class IndexTest {

    private File tempFile;
    private Config config;
    private Index idx;

    @BeforeEach
    void setUp() throws IOException {
        // Create a temporary file for the index
        tempFile = File.createTempFile("index_test", ".tmp");
        tempFile.deleteOnExit();

        // Configure the index
        config = new Config();
        config.segment.maxIndexBytes = 1024;

        // Initialize the index
        idx = new Index(tempFile, config); // assumes constructor handles file and config
    }

    @AfterEach
    void tearDown() throws IOException {
        if (idx != null) {
            idx.close();
        }
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    void testIndexReadWrite() throws IOException {
        // 1. Initially, reading -1 should fail (like Go's io.EOF)
        Exception exception = assertThrows(IOException.class, () -> idx.read(-1));
        assertTrue(exception.getMessage().contains("EOF"));

        // 2. Entries to write
        Index.IndexEntry[] entries = new Index.IndexEntry[] {
                new Index.IndexEntry(0, 0L),
                new Index.IndexEntry(1, 10L)
        };

        // 3. Write entries and read them back
        for (Index.IndexEntry want : entries) {
            idx.write(want.offset(), want.pos());

            Index.IndexEntry result = idx.read(want.offset());
            assertEquals(want.offset(), result.offset(), "Offset should match");
            assertEquals(want.pos(), result.pos(), "Position should match");
        }

        // 4. Reading past the last entry should fail
        int pastOffset = entries.length;
        Exception eofException = assertThrows(IOException.class, () -> idx.read(pastOffset));
        assertTrue(eofException.getMessage().contains("EOF"));

        // 5. Close and reopen index to ensure state is rebuilt from file
        idx.close();
        idx = new Index(tempFile, config);

        Index.IndexEntry lastEntry = idx.read(-1);
        assertEquals(entries[1].offset(), lastEntry.offset(), "Last offset should match");
        assertEquals(entries[1].pos(), lastEntry.pos(), "Last position should match");
    }
}
