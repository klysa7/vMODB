package dk.ku.di.dms.vms.calcite.client;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public class VmsResultIterator implements Iterator<Object[]> {

    private static final System.Logger LOGGER =
            System.getLogger(VmsResultIterator.class.getName());

    private static final byte QUERY_RESULT_TYPE  = 100;
    private static final byte END_OF_STREAM_TYPE = 101;
    private static final byte WORKER_ABORT_TYPE  = 102;
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneOffset.UTC);
    private final Socket socket;
    private final DataInputStream dataInputStream;
    private final List<ColumnDescriptor> descriptors;
    private final String tableName;
    private final Queue<Object[]> rowBuffer = new ArrayDeque<>(1024);
    private boolean isEos = false;
    private long recordsRead = 0;

    public VmsResultIterator(Socket socket, Object inputStream,
                             List<ColumnDescriptor> descriptors, String tableName) {
        this.socket = socket;
        if (inputStream instanceof BufferedInputStream bis) {
            this.dataInputStream = new DataInputStream(bis);
        } else {
            this.dataInputStream = new DataInputStream(
                    new BufferedInputStream((java.io.InputStream) inputStream, 65536));
        }
        this.descriptors = descriptors;
        this.tableName   = tableName;
    }

    public VmsResultIterator(Socket socket, Object inputStream,
                             Class<?>[] columnTypes, String tableName) {
        this(socket, inputStream, (List<ColumnDescriptor>) null, tableName);
    }

    private void fetchNextBatch() {
        if (isEos) return;
        try {
            byte type = dataInputStream.readByte();

            if (type == WORKER_ABORT_TYPE) {
                LOGGER.log(ERROR, ">>> [ITERATOR] Received WORKER_ABORT (102) from VMS for table: "
                        + tableName + ". Worker hit write timeout — gateway likely stalled.");
                close();
                throw new RuntimeException(
                        "VMS worker aborted while streaming table: " + tableName);
            }

            if (type == END_OF_STREAM_TYPE) {
                dataInputStream.readInt();
                dataInputStream.readLong();
                LOGGER.log(INFO, ">>> [ITERATOR] Clean End of stream reached. Total read: "
                        + recordsRead);
                close();
                return;
            }

            if (type == QUERY_RESULT_TYPE) {
                int  dataSize  = dataInputStream.readInt();
                long queryId   = dataInputStream.readLong();
                int  remaining = dataSize - 8;

                while (remaining > 0) {
                    int    rowSize = dataInputStream.readInt();
                    byte[] rowData = new byte[rowSize];
                    dataInputStream.readFully(rowData);
                    remaining -= (4 + rowSize);

                    rowBuffer.add(parseRowData(rowData));
                    recordsRead++;
                }
            }
        } catch (EOFException e) {
            close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.log(ERROR, ">>> [ITERATOR] FATAL ERROR during fetchNextBatch!", e);
            close();
        }
    }

    private Object[] parseRowData(byte[] rowData) {
        if (descriptors == null || descriptors.isEmpty()) return new Object[0];

        Object[] row = new Object[descriptors.size()];
        ByteBuffer buf = ByteBuffer.wrap(rowData).order(ByteOrder.nativeOrder());

        for (int i = 0; i < descriptors.size(); i++) {
            ColumnDescriptor d = descriptors.get(i);

            if (!d.isReadable()) { row[i] = null; continue; }

            int offset = d.byteOffset();
            if (offset + d.byteSize() > rowData.length) { row[i] = null; continue; }

            row[i] = switch (d.type()) {
                case INT                -> buf.getInt(offset);
                case LONG, BIGINT       -> buf.getLong(offset);
                case FLOAT              -> buf.getFloat(offset);
                case DOUBLE             -> buf.getDouble(offset);
                case BOOLEAN, BOOL      -> buf.get(offset) != 0;
                case DATE, TIMESTAMP    -> {
                    long epoch = buf.getLong(offset);
                    yield DATE_FORMATTER.format(Instant.ofEpochMilli(epoch));
                }
                case VARCHAR, STRING, BYTES -> readString(rowData, offset, d.byteSize());
                default                 -> null;
            };
        }

        return row;
    }

    private static String readString(byte[] data, int offset, int size) {
        int end   = offset;
        int limit = Math.min(offset + size, data.length);
        while (end < limit && data[end] != 0) end++;
        if (end <= offset) return "";
        return new String(data, offset, end - offset, StandardCharsets.UTF_8);
    }

    @Override
    public boolean hasNext() {
        if (rowBuffer.isEmpty() && !isEos) fetchNextBatch();
        return !rowBuffer.isEmpty();
    }

    @Override
    public Object[] next() {
        if (!hasNext()) throw new NoSuchElementException();
        return rowBuffer.poll();
    }

    private void close() {
        if (!isEos) {
            isEos = true;
            try { socket.close(); } catch (Exception ignored) {}
        }
    }
}