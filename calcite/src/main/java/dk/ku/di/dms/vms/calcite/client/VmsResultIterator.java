package dk.ku.di.dms.vms.calcite.client;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.ERROR;

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
    }

    private void fetchNextBatch() {
        if (isEos) return;
        try {
            byte type = dataInputStream.readByte();

            if (type == 101) {
                dataInputStream.readInt();
                dataInputStream.readLong();
                LOGGER.log(INFO, ">>> [ITERATOR] Clean End of stream reached. Total read: " + recordsRead);
                close();
                return;
            }

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
                }
            }
        } catch (EOFException e) {
            close();
        } catch (Exception e) {
            LOGGER.log(ERROR, ">>> [ITERATOR] FATAL ERROR during fetchNextBatch!", e);
            close();
        }
    }

    private Object[] parseRowData(byte[] rowData) {
        Object[] row = new Object[columnTypes.length]; // Calcite makes this length 22

        // VMODB Physical Memory uses Native Order (Little Endian on most systems)
        ByteBuffer wrapper = ByteBuffer.wrap(rowData).order(ByteOrder.nativeOrder());

        try {
            if (rowData.length > 100) {

                // Index 0: c_id (Explicit Join Key at the very front)
                row[0] = wrapper.getInt(0);

                // Index 3: c_first (Calcite expects this at index 3)
                row[3] = "N/A";

                // Index 14: o_id (Calcite expects this at index 14)
                int orderRecordSize = 36;
                int orderStartOffset = rowData.length - orderRecordSize;
                row[14] = wrapper.getInt(orderStartOffset);

                return row;
            }

            Arrays.fill(row, "SINGLE_TABLE_NOT_SUPPORTED_HERE");
        } catch (Exception e) {
            Arrays.fill(row, "PARSE_ERROR");
        }

        return row;
    }

    @Override
    public boolean hasNext() {
        if (rowBuffer.isEmpty() && !isEos) {
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