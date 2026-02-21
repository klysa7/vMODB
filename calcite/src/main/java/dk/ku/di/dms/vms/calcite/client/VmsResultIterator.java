package dk.ku.di.dms.vms.calcite.client;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import static java.lang.System.Logger.Level.INFO;

public class VmsResultIterator implements Iterator<Object[]> {

    private static final System.Logger LOGGER = System.getLogger(VmsResultIterator.class.getName());

    private final Socket socket;
    private final DataInputStream dataInputStream;
    private final Class<?>[] columnTypes;
    private final String tableName;
    private final Queue<Object[]> rowBuffer = new LinkedList<>();
    private boolean isEos = false;
    private long recordsRead = 0;

    public VmsResultIterator(Socket socket, Object inputStream, Class<?>[] columnTypes, String tableName) {
        this.socket = socket;
        if (inputStream instanceof BufferedInputStream) {
            this.dataInputStream = new DataInputStream((BufferedInputStream) inputStream);
        } else {
            this.dataInputStream = new DataInputStream(new BufferedInputStream((java.io.InputStream) inputStream, 65536));
        }
        this.columnTypes = columnTypes;
        this.tableName = tableName;

        LOGGER.log(INFO, "Iterator initialized for: " + tableName);
    }

    private void fetchNextBatch() {
        if (isEos) return;
        try {
            byte type = dataInputStream.readByte();

            //end of stream
            if (type == 101) {
                dataInputStream.readInt();
                dataInputStream.readLong();
                LOGGER.log(INFO, "[" + tableName + "] End of stream reached. Total read: " + recordsRead);
                close();
                return;
            }
            //QUERY_RESULT_TYPE
            if (type == 100) {
                int dataSize = dataInputStream.readInt();
                long queryId = dataInputStream.readLong();
                int bytesRemainingInBatch = dataSize - 8;

                while (bytesRemainingInBatch > 0) {
                    int rowSize = dataInputStream.readInt();
                    byte[] rowData = new byte[rowSize];
                    dataInputStream.readFully(rowData);

                    bytesRemainingInBatch -= (4 + rowSize);

                    Object[] parsedRow = parseRowData(rowData);
                    rowBuffer.add(parsedRow);
                    recordsRead++;

                    if (recordsRead <= 3 || recordsRead % 10000 == 0) {
                        LOGGER.log(INFO, "[" + tableName + "] Scanned Record " + recordsRead);
                    }
                }
            }
        } catch (EOFException e) {
            close();
        } catch (Exception e) {
            LOGGER.log(INFO, "Iterator error", e);
            close();
        }
    }

    private Object[] parseRowData(byte[] rowData) {
        Object[] row = new Object[columnTypes.length];
        ByteBuffer wrapper = ByteBuffer.wrap(rowData).order(ByteOrder.LITTLE_ENDIAN);
//very specific, maybe find the tables, columnns in a dynamic way
        if (tableName.contains("customer")) {
            int base = 17;
            try {
                row[2] = wrapper.getInt(base + 0);
                row[1] = wrapper.getInt(base + 4);
                row[0] = wrapper.getInt(base + 8);

                if (columnTypes.length > 3) {
                    byte[] strBytes = new byte[16];
                    wrapper.position(base + 12);
                    wrapper.get(strBytes);
                    row[3] = new String(strBytes, StandardCharsets.UTF_8).trim();
                }
            } catch (Exception e) { Arrays.fill(row, "RAW"); }
        } else {//order
            int base = 4;
            try {
                int w_id = wrapper.getInt(base + 0);
                int d_id = wrapper.getInt(base + 4);
                int o_id = wrapper.getInt(base + 8);
                int c_id = wrapper.getInt(base + 12);

                row[2] = w_id;
                row[1] = d_id;
                row[0] = o_id;
                row[3] = c_id;

                if (columnTypes.length > 4) row[4] = wrapper.getLong(base + 16);
            } catch (Exception e) { Arrays.fill(row, "RAW"); }
        }
        return row;
    }

    @Override
    public boolean hasNext() {
        if (rowBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !rowBuffer.isEmpty();
    }

    @Override
    public Object[] next() {
        if (!hasNext()) throw new NoSuchElementException();
        return rowBuffer.poll();
    }

    private void close() {
        if(!isEos) {
            isEos = true;
            try { socket.close(); } catch(Exception ignored){}
        }
    }
}