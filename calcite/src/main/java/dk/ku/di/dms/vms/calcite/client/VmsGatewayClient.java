package dk.ku.di.dms.vms.calcite.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class VmsGatewayClient {

    private static final byte GATEWAY_TYPE = 12;
    private static final byte QUERY_REQUEST_TYPE = 99;

    public Iterator<Object[]> scan(String host, int port, long queryId, long snapshotId, byte mode, String tableName, List<Class<?>> columnTypes, byte[] predicates, byte[] routingData) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            buildPayload(buffer, queryId, snapshotId, mode, tableName, predicates, routingData);

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            return new VmsResultIterator(socket, in, columnTypes.toArray(new Class<?>[0]), tableName);

        } catch (IOException e) {
            throw new RuntimeException("Connection failed: " + host + ":" + port, e);
        }
    }

    public void triggerBroadcast(String host, int port, long queryId, long snapshotId, String tableName, byte[] predicates, String targetHostPort) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            buildPayload(buffer, queryId, snapshotId, (byte) 1, tableName, predicates, targetHostPort.getBytes(StandardCharsets.UTF_8));

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            System.out.println(">>> [GATEWAY] Trigger sent! Waiting 5 seconds for VMS to finish...");
            Thread.sleep(5000);

            socket.close();
            System.out.println(">>> [GATEWAY] Socket closed cleanly.");
        } catch (Exception e) {
            System.err.println("Broadcast trigger failed: " + e.getMessage());
        }
    }

    private void buildPayload(ByteBuffer buffer, long queryId, long snapshotId, byte mode, String tableName, byte[] predicates, byte[] routingData) {
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