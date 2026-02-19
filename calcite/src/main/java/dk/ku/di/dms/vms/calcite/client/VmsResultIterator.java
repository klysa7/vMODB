package dk.ku.di.dms.vms.calcite.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public class VmsResultIterator implements Iterator<Object[]> {

    private static final System.Logger LOGGER = System.getLogger(VmsResultIterator.class.getName());

    private final Socket socket;
    private final BufferedInputStream in;
    private final Class<?>[] columnTypes;
    private final String tableName;
    private final byte[] streamBuffer = new byte[8192];
    private int bufferPos = 0;
    private int bufferLimit = 0;
    private Object[] nextRow;
    private boolean hasNext = false;
    private boolean closed = false;
    private long recordsRead = 0;
    private static final int SIG_1 = 0x64;
    private static final int SIG_2 = 0x00;
    private static final int SIG_3 = 0x00;
    private static final int SIG_4 = 0x0F;

    public VmsResultIterator(Socket socket, Object inputStream, Class<?>[] columnTypes, String tableName) {
        this.socket = socket;
        if (inputStream instanceof BufferedInputStream) {
            this.in = (BufferedInputStream) inputStream;
        } else {
            this.in = new BufferedInputStream((java.io.InputStream) inputStream);
        }
        this.columnTypes = columnTypes;
        this.tableName = tableName;

        LOGGER.log(INFO, "Self-Healing Iterator initialized for: " + tableName);
        advance();
    }

    private void advance() {
        if (closed) return;
        try {
            while (true) {
                if (!ensureBuffer(4)) {
                    close(); return;
                }

                int b1 = streamBuffer[bufferPos] & 0xFF;
                int b2 = streamBuffer[bufferPos+1] & 0xFF;
                int b3 = streamBuffer[bufferPos+2] & 0xFF;
                int b4 = streamBuffer[bufferPos+3] & 0xFF;

                if (b1 == SIG_1 && b2 == SIG_2 && b3 == SIG_3 && b4 == SIG_4) {
                    parseFoundRecord();
                    return;
                } else {
                    bufferPos++;
                }
            }
        } catch (Exception e) {
            close();
        }
    }

    private void parseFoundRecord() {
        recordsRead++;

        // We are at the start of the record (Header).
        // Based on previous analysis:
        // Customer Header Offset = 17 (from start of signature)
        // Order Header Offset = 4 (immediately after signature)

        // Ensure we have enough data in buffer to parse columns
        // Max estimate: Customer ~700 bytes, Order ~50 bytes
        int needed = tableName.contains("customer") ? 700 : 64;
        if (!ensureBuffer(needed)) {
            close(); return;
        }

        Object[] row = new Object[columnTypes.length];

        // Create a wrapper for this specific chunk
        // Note: bufferPos is currently at the signature
        ByteBuffer wrapper = ByteBuffer.wrap(streamBuffer, bufferPos, needed).order(ByteOrder.LITTLE_ENDIAN);

        if (tableName.contains("customer")) {
            // Offset 17 relative to bufferPos
            int base = 17;
            try {
                if (columnTypes.length > 0) row[0] = wrapper.getInt(wrapper.position() + base + 0);
                if (columnTypes.length > 1) row[1] = wrapper.getInt(wrapper.position() + base + 4);
                if (columnTypes.length > 2) row[2] = wrapper.getInt(wrapper.position() + base + 8);
                // Strings
                if (columnTypes.length > 3) row[3] = readString(wrapper, base + 12, 16);
            } catch (Exception e) { fillDummy(row); }
        } else {
            // Order: Offset 4 relative to bufferPos (Right after 4-byte header)
            int base = 4;
            try {
                if (columnTypes.length > 0) row[0] = wrapper.getInt(wrapper.position() + base + 0);
                if (columnTypes.length > 1) row[1] = wrapper.getInt(wrapper.position() + base + 4);
                if (columnTypes.length > 2) row[2] = wrapper.getInt(wrapper.position() + base + 8);
                if (columnTypes.length > 3) row[3] = wrapper.getInt(wrapper.position() + base + 12);
                if (columnTypes.length > 4) row[4] = wrapper.getLong(wrapper.position() + base + 16);
            } catch (Exception e) { fillDummy(row); }
        }

        // Move bufferPos forward to avoid re-reading this signature
        // We jump by a safe minimum amount to start scanning for next
        bufferPos += (tableName.contains("customer") ? 600 : 32);

        nextRow = row;
        hasNext = true;

        if (recordsRead <= 3 || recordsRead % 10000 == 0) {
            LOGGER.log(INFO, "[" + tableName + "] Scanned Record " + recordsRead + ": " + Arrays.toString(row));
        }
    }

    private boolean ensureBuffer(int bytesNeeded) {
        // Compact buffer if needed
        if (bufferLimit - bufferPos < bytesNeeded) {
            // Move remaining bytes to start
            int remaining = bufferLimit - bufferPos;
            if (remaining > 0) {
                System.arraycopy(streamBuffer, bufferPos, streamBuffer, 0, remaining);
            }
            bufferPos = 0;
            bufferLimit = remaining;

            // Read more
            try {
                int read = in.read(streamBuffer, bufferLimit, streamBuffer.length - bufferLimit);
                if (read == -1) return false;
                bufferLimit += read;
            } catch (IOException e) {
                return false;
            }
        }
        return (bufferLimit - bufferPos) >= bytesNeeded;
    }

    private String readString(ByteBuffer wrap, int offset, int len) {
        byte[] b = new byte[len];
        // Absolute read from backing array logic
        int start = wrap.position() + offset;
        System.arraycopy(streamBuffer, start, b, 0, len);
        return new String(b, StandardCharsets.UTF_8).trim();
    }

    private void fillDummy(Object[] row) { Arrays.fill(row, "RAW"); }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Object[] next() {
        if(!hasNext){
            throw new NoSuchElementException();
        }
        Object[] r = nextRow;
        hasNext = false;
        advance();
        return r;
    }

    private void close() {
        if(!closed) {
            closed=true;
            hasNext=false;
            try{
                socket.close();
            } catch(Exception e){

            }
        }
    }
}