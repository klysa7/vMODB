package dk.ku.di.dms.vms.calcite.client;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;

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

    public Iterator<Object[]> scan(String host, int port, long queryId, String tableName, List<Class<?>> columnTypes) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            ByteBuffer buffer = ByteBuffer.allocate(4096);
            buffer.put(Constants.PRESENTATION);
            buffer.put(GATEWAY_TYPE);
            writeString(buffer, "Gateway");
            buffer.putLong(0L);
            buffer.putLong(0L);
            buffer.putLong(0L);
            buffer.putInt(0);
            writeString(buffer, "localhost");

            byte[] emptyJson = "{}".getBytes(StandardCharsets.UTF_8);
            for(int i=0; i<3; i++) {
                buffer.putInt(emptyJson.length);
                buffer.put(emptyJson);
            }

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            try { Thread.sleep(200); } catch (InterruptedException ignored) {}

            buffer.clear();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buffer.putLong(queryId);
            buffer.putInt(0);
            writeString(buffer, "");
            writeString(buffer, tableName);
            buffer.putInt(0);

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            return new VmsResultIterator(socket, in, columnTypes.toArray(new Class<?>[0]), tableName);

        } catch (IOException e) {
            throw new RuntimeException("Connection failed: " + host + ":" + port, e);
        }
    }

    private void writeString(ByteBuffer buffer, String val) {
        if (val == null) val = "";
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }
}