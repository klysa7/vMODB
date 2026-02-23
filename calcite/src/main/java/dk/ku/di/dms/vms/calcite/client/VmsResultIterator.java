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
        LOGGER.log(INFO, ">>> [ITERATOR] Initialized. Awaiting bytes from: " + tableName);
    }

    private void fetchNextBatch() {
        if (isEos) return;
        try {
            LOGGER.log(INFO, ">>> [ITERATOR] Blocking on network read for table: " + tableName);
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

                    if (recordsRead <= 3 || recordsRead % 10000 == 0) {
                        LOGGER.log(INFO, ">>> [ITERATOR] Successfully extracted JSON Record " + recordsRead);
                    }
                }
            }
        } catch (EOFException e) {
            LOGGER.log(INFO, ">>> [ITERATOR] Connection closed normally by VMS.");
            close();
        } catch (Exception e) {
            LOGGER.log(ERROR, ">>> [ITERATOR] FATAL ERROR during fetchNextBatch!", e);
            close();
        }
    }

    private Object[] parseRowData(byte[] rowData) {
        Object[] row = new Object[columnTypes.length];
        ByteBuffer wrapper = ByteBuffer.wrap(rowData).order(ByteOrder.LITTLE_ENDIAN);

        try {
            // THE DISTRIBUTED JOIN RESPONSE
            if (rowData.length > 100) {
                int customerBase = 17;
                row[0] = wrapper.getInt(customerBase + 8); // c_id

                byte[] strBytes = new byte[16];
                wrapper.position(customerBase + 12);
                wrapper.get(strBytes);
                row[1] = new String(strBytes, StandardCharsets.UTF_8).trim(); // c_first

                int orderRecordSize = 40;
                int orderStartOffset = rowData.length - orderRecordSize;
                int orderBase = orderStartOffset + 4;
                row[2] = wrapper.getInt(orderBase + 8); // o_id

                return row;
            }

            // STANDARD SINGLE TABLE SCANS
            if (tableName.contains("customer")) {
                int base = 17;
                row[2] = wrapper.getInt(base + 0);
                row[1] = wrapper.getInt(base + 4);
                row[0] = wrapper.getInt(base + 8);

                if (columnTypes.length > 3) {
                    byte[] strBytes = new byte[16];
                    wrapper.position(base + 12);
                    wrapper.get(strBytes);
                    row[3] = new String(strBytes, StandardCharsets.UTF_8).trim();
                }
            } else { // order
                int base = 4;
                int w_id = wrapper.getInt(base + 0);
                int d_id = wrapper.getInt(base + 4);
                int o_id = wrapper.getInt(base + 8);
                int c_id = wrapper.getInt(base + 12);

                row[2] = w_id;
                row[1] = d_id;
                row[0] = o_id;
                row[3] = c_id;

                if (columnTypes.length > 4) row[4] = wrapper.getLong(base + 16);
            }
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