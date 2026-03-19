package dk.ku.di.dms.vms.calcite.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VmsGatewayClient {

    private static final byte GATEWAY_TYPE       = 12;
    private static final byte QUERY_REQUEST_TYPE = 99;

    public Iterator<Object[]> scan(String host, int port, long queryId, long snapshotId,
                                   byte mode, String tableName,
                                   List<Class<?>> columnTypes,
                                   byte[] predicates, byte[] routingData) {
        return scanWithSchema(host, port, queryId, snapshotId, mode, tableName,
                null, predicates, routingData);
    }

    public Iterator<Object[]> scanWithSchema(String host, int port, long queryId, long snapshotId,
                                             byte mode, String tableName,
                                             List<ColumnDescriptor> descriptors,
                                             byte[] predicates, byte[] routingData) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            buildPayload(buffer, queryId, snapshotId, mode, tableName, predicates, routingData);

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            return new VmsResultIterator(socket, in, descriptors, tableName);

        } catch (IOException e) {
            throw new RuntimeException("Connection failed: " + host + ":" + port, e);
        }
    }

    // -------------------------------------------------------------------------
    // B4 FIX: Replace the 5-second sleep with immediate socket close.
    //
    // OLD: after flushing the mode=1 (BROADCAST_TO_VMS) command, the gateway
    //      slept for 5000 ms as a timing workaround, then closed the socket.
    //      If the VMS takes longer than 5 s the gateway closes mid-broadcast.
    //      If it finishes faster, 5 s of wall time is wasted per query.
    //
    // NEW: close the socket immediately after flush. The VMS has already
    //      received the complete command payload. It independently opens its
    //      own AsynchronousSocketChannel connection to the probe VMS and
    //      streams rows there — the original gateway socket is not used for
    //      data transfer at all. No sleep is needed.
    //
    //      The DistributedExecutor already calls scanWithSchema (mode=2) on
    //      the probe VMS before triggerBroadcast, so the probe VMS has its
    //      JoinContext registered and is ready to receive rows. The gateway's
    //      VmsResultIterator on that second connection blocks naturally on
    //      VmsResultIterator.fetchNextBatch() until the join result flows —
    //      no timing assumption anywhere in the pipeline.
    // -------------------------------------------------------------------------
    public void triggerBroadcast(String host, int port, long queryId, long snapshotId,
                                 String tableName, byte[] predicates, String targetHostPort) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            buildPayload(buffer, queryId, snapshotId, (byte) 1, tableName,
                    predicates, targetHostPort.getBytes(StandardCharsets.UTF_8));

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            // B4 FIX: close immediately — the VMS received the full command and
            // will independently connect to the probe VMS. No sleep needed.
            socket.close();
            System.out.println(">>> [GATEWAY] Broadcast trigger sent to " + host + ":" + port
                    + " for table " + tableName + " (queryId=" + queryId + ")");

        } catch (Exception e) {
            System.err.println("Broadcast trigger failed: " + e.getMessage());
        }
    }

    private void buildPayload(ByteBuffer buffer, long queryId, long snapshotId,
                              byte mode, String tableName,
                              byte[] predicates, byte[] routingData) {
        int startPos = buffer.position();
        buffer.put(QUERY_REQUEST_TYPE);
        buffer.putInt(0);

        buffer.putLong(queryId);
        buffer.putLong(snapshotId);
        buffer.put(mode);

        byte[] tableBytes = tableName.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(tableBytes.length);
        buffer.put(tableBytes);

        if (predicates != null && predicates.length > 0) {
            buffer.putInt(predicates.length);
            buffer.put(predicates);
        } else {
            buffer.putInt(0);
        }

        if (routingData != null && routingData.length > 0) {
            buffer.putInt(routingData.length);
            buffer.put(routingData);
        } else {
            buffer.putInt(0);
        }

        int endPos = buffer.position();
        buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
        buffer.position(endPos);
    }
}